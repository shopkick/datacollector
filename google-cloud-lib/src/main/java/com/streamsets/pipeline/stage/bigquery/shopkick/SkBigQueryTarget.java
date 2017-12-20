package com.streamsets.pipeline.stage.bigquery.shopkick;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.cloud.bigquery.BigQuery.TableField;
import com.google.cloud.bigquery.BigQuery.TableOption;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Field.Type;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.stage.bigquery.destination.BigQueryTarget;
import com.streamsets.pipeline.stage.bigquery.destination.BigQueryTargetConfig;
import com.streamsets.pipeline.stage.bigquery.lib.Errors;

public class SkBigQueryTarget extends BigQueryTarget {
  private static final int RETRY_SLEEP_TIME_MS = 500;
  private static final String REASON_STOPPED = "stopped";
  private static final String REASON_INVALID = "invalid";
  private static final String NO_SUCH_FIELD = "no such field.";

  private static final int PARTITION_DATE_SUFFIX_LEN = 9;
  private static final char ARRAY_START_CHAR = '[';
  private static final char FORWARD_SLASH_CHAR = '/';
  private static final String FORWARD_SLASH = FORWARD_SLASH_CHAR + "";
  private static final String ERR_BQ_AUTO_CREATE_TABLE = "ERR_BQ_AUTO_CREATE_TABLE";
  private static final String ERR_BQ_AUTO_ADD_COLUMNS = "ERR_BQ_AUTO_ADD_COLUMNS";
  private static final String AUTO_ADD_COLUMNS_INSERT_FAILURE = "AUTO_ADD_COLUMNS_INSERT_FAILURE";
  private static final Logger LOG = LoggerFactory.getLogger(SkBigQueryTarget.class);

  private static final Pattern PARTITION_PATTERN = Pattern.compile("\\$\\d{4}\\d{2}\\d{2}$");
  private static final int ADD_COLS_RETRIES = 10;
  private SkBigQueryTargetConfig conf;
  private int maxWaitTimeForInsertMins;
  private boolean retryForInsertErrors;

  public SkBigQueryTarget(SkBigQueryTargetConfig conf, BigQueryTargetConfig bqConf) {
    super(bqConf);
    this.conf = conf;
    this.maxWaitTimeForInsertMins = conf.maxWaitTimeForInsertMins;
    this.retryForInsertErrors = conf.autoAddRetryHandler == AutoAddColRetryHandler.BLOCKING;
  }

  @Override
  protected void handleTableNotFound(Record record, String datasetName, String tableName,
      Map<TableId, List<Record>> tableIdToRecords) {
    if (!conf.autoAddTable) {
      super.handleTableNotFound(record, datasetName, tableName, null);
      return;
    }

    Result result = null;
    try {
      if (bigQuery.getDataset(datasetName) == null) {
        bigQuery.create(DatasetInfo.of(datasetName));
        LOG.info("Dataset {} not found, created", datasetName);
      }
      TableId tableId = TableId.of(datasetName, tableName);
      result = createTable(record, tableId);
      if (result.result) {
        // Success, re-add the record to the list
        List<Record> tableIdRecords =
            tableIdToRecords.computeIfAbsent(tableId, t -> new ArrayList<>());
        tableIdRecords.add(record);
        refreshTableIdExistsCache(tableId);
      }
    } catch (Exception e) {
      setErrorAttribute(ERR_BQ_AUTO_CREATE_TABLE, record, e.getMessage());
      LOG.info("Exception in big query auto create table {}.{}", datasetName, tableName, e);
      super.handleTableNotFound(record, datasetName, tableName, null);
      return;
    }
    if (result == null || !result.result) {
      setErrorAttribute(ERR_BQ_AUTO_CREATE_TABLE, record, result.message);
      LOG.info("Auto create failed for table {}.{}. Message: {}", datasetName, tableName,
          result.message);
      super.handleTableNotFound(record, datasetName, tableName, null);
    }
  }

  @Override
  protected void handleInsertErrors(TableId tableId, ELVars elVars,
      Map<Long, Record> requestIndexToRecords, InsertAllResponse response) {
    if (!conf.autoAddTable) {
      super.reportErrors(requestIndexToRecords, response);
    }

    List<Record> retry = new ArrayList<>();
    List<ErrorRecord> stopped = new ArrayList<>();
    List<ErrorRecord> missingCols = new ArrayList<>();

    bucketizeErrors(requestIndexToRecords, response, stopped, missingCols);

    if (!missingCols.isEmpty()) {
      addMissingColumnsInBigQuery(tableId, retry, missingCols);
    }

    if (!retry.isEmpty()) {
      insertBatchAgain(tableId, elVars, requestIndexToRecords, retry, stopped);
    } else {
      if (!stopped.isEmpty()) {
        stopped.forEach(r -> super.handleInsertError(r.record, r.messages, r.reasons));
      }
    }
  }

  private void insertBatchAgain(TableId tableId, ELVars elVars,
      Map<Long, Record> requestIndexToRecords, List<Record> retry, List<ErrorRecord> stopped) {
    InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
    insertAllRequestBuilder.setIgnoreUnknownValues(false);
    insertAllRequestBuilder.setSkipInvalidRows(false);
    addToInsertRequest(elVars, retry, insertAllRequestBuilder);

    if (!stopped.isEmpty()) {
      addToInsertRequest(elVars, stopped.stream().map(e -> e.record).collect(Collectors.toList()),
          insertAllRequestBuilder);
    }

    InsertAllRequest req = insertAllRequestBuilder.build();

    insertAllWithRetries(requestIndexToRecords, elVars, tableId, req, 1, 0);
  }

  private void addMissingColumnsInBigQuery(TableId tableId, List<Record> retry,
      List<ErrorRecord> missingCols) {
    missingCols.forEach(err -> {
      Result added = addMissingColumnsInBigQuery(tableId, err.record);
      if (added.result) {
        retry.add(err.record);
      } else {
        setErrorAttribute(ERR_BQ_AUTO_ADD_COLUMNS, err.record, added.message);
        super.handleInsertError(err.record, err.messages, err.reasons);
      }
    });
  }

  private void insertAllWithRetries(Map<Long, Record> requestIndexToRecords, ELVars elVars,
      TableId tableId, InsertAllRequest request, int sleepTimeSec, int elapsedSec) {

    int elapsedMin = elapsedSec / 60;
    if (elapsedMin > maxWaitTimeForInsertMins) {
      LOG.warn("Cannot Send message through retries. Elapsed: {} Secs. Trying once more",
          elapsedSec);
      addRecordValueToHeader(requestIndexToRecords);
      insertAll(requestIndexToRecords, elVars, tableId, request, false);
      return;
    }

    LOG.debug("InsertAll Request. TableId: {}, Request: {}", tableId, request);
    if (!request.getRows().isEmpty()) {
      try {
        InsertAllResponse response = bigQuery.insertAll(request);
        if (response.hasErrors()) {

          List<ErrorRecord> stopped = new ArrayList<>();
          List<ErrorRecord> missingCols = new ArrayList<>();
          bucketizeErrors(requestIndexToRecords, response, stopped, missingCols);
          if (missingCols.size() > 0) {
            if (!retryForInsertErrors) {
              LOG.debug(
                  "Auto Add Col Insert Error Retry disabled, sending record to error handler");
              addRecordValueToHeader(requestIndexToRecords);
              super.reportErrors(requestIndexToRecords, response);
              return;
            }
            sleep(sleepTimeSec);
            insertAllWithRetries(requestIndexToRecords, elVars, tableId, request,
                sleepTimeSec + sleepTimeSec, elapsedSec + sleepTimeSec);
          } else {
            LOG.warn("Cannot retry for unknown errors");
            super.reportErrors(requestIndexToRecords, response);
            return;
          }
        } else {
          LOG.debug("insertAllWithRetries Success");
        }
      } catch (BigQueryException e) {
        LOG.error(Errors.BIGQUERY_13.getMessage(), e);
        // Put all records to error.
        for (long i = 0; i < request.getRows().size(); i++) {
          Record record = requestIndexToRecords.get(i);
          getContext().toError(record, Errors.BIGQUERY_13, e);
        }
      }
    }
  }

  private void addRecordValueToHeader(Map<Long, Record> requestIndexToRecords) {
    Iterator<Entry<Long, Record>> iterator = requestIndexToRecords.entrySet().iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next().getValue();
      record.getHeader().setAttribute(AUTO_ADD_COLUMNS_INSERT_FAILURE, "");
    }
  }

  private void sleep(int sleepTimeSec) {
    try {
      LOG.info("Sleeping {} seconds before retry", sleepTimeSec);
      TimeUnit.SECONDS.sleep(sleepTimeSec);
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }
  }

  private void addToInsertRequest(ELVars elVars, List<Record> retry,
      InsertAllRequest.Builder insertAllRequestBuilder) {
    retry.forEach(record -> {
      try {
        String insertId = getInsertIdForRecord(elVars, record);
        Map<String, ?> rowContent = convertToRowObjectFromRecord(record);
        if (rowContent.isEmpty()) {
          throw new OnRecordErrorException(record, Errors.BIGQUERY_14);
        }
        insertAllRequestBuilder.addRow(insertId, rowContent);
      } catch (OnRecordErrorException e) {
        LOG.error("Error when converting record {} to row, Reason : {} ",
            record.getHeader().getSourceId(), e.getMessage());
        getContext().toError(record, e.getErrorCode(), e.getParams());
      }
    });
  }

  private void bucketizeErrors(Map<Long, Record> requestIndexToRecords, InsertAllResponse response,
      List<ErrorRecord> stopped, List<ErrorRecord> missingCols) {
    response.getInsertErrors().forEach((requestIdx, errors) -> {
      Record record = requestIndexToRecords.get(requestIdx);
      String messages = COMMA_JOINER
          .join(errors.stream().map(BigQueryError::getMessage).collect(Collectors.toList()));
      String reasons = COMMA_JOINER
          .join(errors.stream().map(BigQueryError::getReason).collect(Collectors.toList()));
      String locations = COMMA_JOINER
          .join(errors.stream().map(BigQueryError::getLocation).collect(Collectors.toList()));
      LOG.debug(
          "<Auto_add_column> Handling Error when inserting record {}, Reasons : {}, Messages : {}, Locations: {}",
          record.getHeader().getSourceId(), reasons, messages, locations);
      if (REASON_STOPPED.equalsIgnoreCase(reasons)) {
        stopped.add(new ErrorRecord(record, messages, reasons));
      } else if (REASON_INVALID.equalsIgnoreCase(reasons)
          && NO_SUCH_FIELD.equalsIgnoreCase(messages)) {
        missingCols.add(new ErrorRecord(record, messages, reasons));
      } else {
        super.handleInsertError(record, messages, reasons);
      }
    });
  }

  private Result addMissingColumnsInBigQuery(TableId tableId, Record record) {
    String tableName = tableId.getTable();
    if (isPartitioned(tableName)) {
      tableName = extractTableName(tableName);
    }

    // Table ID without partition suffix
    TableId tableIdOnly = TableId.of(tableId.getDataset(), tableName);

    Table table = bigQuery.getTable(tableIdOnly);
    Result result = getAllColumns(table, record);
    if (!result.result) {
      return result;
    }

    if (!result.fields.isEmpty()) {
      Schema schema = Schema.of(result.fields);
      TableInfo tableInfo =
          table.toBuilder().setDefinition(StandardTableDefinition.of(schema)).build();
      Table update = bigQuery.update(tableInfo);
      LOG.info("Table updated with additional fields: " + update);

      boolean updateDone = isTableUpdated(update, tableId, result);

      if (!updateDone) {
        String errMsg = String.format("Update not getting reflected in table %s", tableId);
        LOG.warn(errMsg);
        return new Result(false, errMsg);
      }
    }

    return new Result();
  }

  private boolean isTableUpdated(Table updateResponse, TableId tableId, Result result) {
    int retryCount = 0;

    boolean updateDone = checkTableHasFields(updateResponse, result.fields);
    while (!updateDone && retryCount < ADD_COLS_RETRIES) {
      try {
        LOG.debug("Sleeping for {} seconds for checking whether table {} updated",
            RETRY_SLEEP_TIME_MS, tableId);
        TimeUnit.MILLISECONDS.sleep(RETRY_SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted", e);
      }
      updateDone = checkTableHasFields(tableId, result.fields);
      retryCount++;
    }
    return updateDone;
  }

  private boolean checkTableHasFields(TableId tableId,
      List<com.google.cloud.bigquery.Field> allFields) {
    return checkTableHasFields(bigQuery.getTable(tableId, TableOption.fields(TableField.SCHEMA)),
        allFields);
  }

  private boolean checkTableHasFields(Table table,
      List<com.google.cloud.bigquery.Field> allFields) {
    List<com.google.cloud.bigquery.Field> tableFields =
        table.getDefinition().getSchema().getFields();
    return tableFields.containsAll(allFields);
  }

  protected void sleep() {
    try {
      LOG.debug("Sleeping for seconds before calling insertAll again");
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }
  }

  private Result getAllColumns(Table table, Record record) {
    List<com.google.cloud.bigquery.Field> bqFields = table.getDefinition().getSchema().getFields();
    List<com.google.cloud.bigquery.Field> additional = new ArrayList<>();
    Result ssFields = convertSsToBqFields(record);
    if (!ssFields.result) {
      setErrorAttribute(ERR_BQ_AUTO_ADD_COLUMNS, record, ssFields.message);
      LOG.error(ssFields.message);
      return ssFields;
    }
    Map<String, com.google.cloud.bigquery.Field> fieldMap = new HashMap<>();
    bqFields.forEach(f -> fieldMap.put(f.getName(), f));
    for (com.google.cloud.bigquery.Field field : ssFields.fields) {
      String fieldName = field.getName();
      if (fieldMap.containsKey(fieldName)) {
        com.google.cloud.bigquery.Field.Type ssFieldType = field.getType();
        com.google.cloud.bigquery.Field.Type bqFieldType = fieldMap.get(fieldName).getType();
        if (!ssFieldType.equals(bqFieldType)) {
          String errorMsg = String.format(
              "Type mismatch while trying to auto add column: In Streamsets: %s, In BigQuery: %s",
              ssFieldType, bqFieldType);
          LOG.error(errorMsg);
          return new Result(false, errorMsg);
        }
      } else {
        additional.add(field);
      }
    }
    if (!additional.isEmpty()) {
      additional.addAll(bqFields);
    }
    return new Result(additional);
  }

  private void setErrorAttribute(String attribute, Record record, String message) {
    String toStamp = message != null ? message : "Unknown";
    record.getHeader().setAttribute(attribute, toStamp);
  }

  private Result createTable(Record record, TableId tableId) {
    Result fieldsResult = convertSsToBqFields(record);
    if (!fieldsResult.result) {
      return fieldsResult;
    }
    createTable(tableId, fieldsResult.fields);
    return new Result();
  }

  private Result convertSsToBqFields(Record record) {
    List<com.google.cloud.bigquery.Field> fields = new ArrayList<>();
    Set<String> fieldPaths = record.getEscapedFieldPaths();
    for (String fieldPath : fieldPaths) {
      if (!fieldPathValid(fieldPath))
        continue;
      Field field = record.get(fieldPath);
      LOG.debug("Converting ss field {} with type {} to bq field", fieldPath, field.getType());
      com.google.cloud.bigquery.Field bqField = convertSsToBqField(field, fieldPath);
      if (bqField == null) {
        Result result = new Result(false,
            String.format("Unsupported datatype: %s for auto create", field.getType()));
        return result;
      }
      fields.add(bqField);
    }
    return new Result(fields);
  }

  private void createTable(TableId tableId, List<com.google.cloud.bigquery.Field> fields) {
    Schema schema = Schema.of(fields);
    TableInfo tableInfo = null;
    String tableName = tableId.getTable();
    if (isPartitioned(tableName)) {
      TableDefinition tableDefinition =
          StandardTableDefinition.of(schema).toBuilder().setTimePartitioning(
              TimePartitioning.of(com.google.cloud.bigquery.TimePartitioning.Type.DAY)).build();
      String onlyTableName = extractTableName(tableName);
      tableInfo = TableInfo
          .newBuilder(TableId.of(tableId.getDataset(), onlyTableName), tableDefinition).build();
    } else {
      tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(schema)).build();
    }
    Table table = bigQuery.create(tableInfo);
    LOG.info("Table {} not found, created", tableInfo.getTableId());
    LOG.debug("Table details {}", table);
  }

  protected String extractTableName(String tableName) {
    return tableName.substring(0, tableName.length() - PARTITION_DATE_SUFFIX_LEN);
  }

  private boolean isPartitioned(String table) {
    return PARTITION_PATTERN.matcher(table).find();
  }

  private boolean fieldPathValid(String fieldPath) {
    if (fieldPath.trim().isEmpty()) {
      return false;
    }

    for (int i = 1; i < fieldPath.length(); i++) {
      char charAt = fieldPath.charAt(i);
      if (charAt == FORWARD_SLASH_CHAR || charAt == ARRAY_START_CHAR) {
        return false;
      }
    }

    return true;
  }

  /**
   * Convert Streamsets field type to Bigquery field type along with value
   * 
   * @param fieldPath
   */
  private com.google.cloud.bigquery.Field convertSsToBqField(Field field, String fieldPath) {
    String fieldName = fieldPath.replaceFirst(FORWARD_SLASH, "");

    if (field.getType().equals(Type.LIST)) {
      return getFieldForList(field, fieldName);
    }

    com.google.cloud.bigquery.Field.Type bqFieldType = DATA_TYPE_MAP.get(field.getType());
    if (bqFieldType != null) {
      return com.google.cloud.bigquery.Field.of(fieldName, bqFieldType);
    }
    return null;
  }

  private com.google.cloud.bigquery.Field getFieldForList(Field field, String fieldPath) {
    List<Field> values = field.getValueAsList();
    if (values.size() == 0) {
      LOG.debug("List: {} empty, cannot determine data type. Cannot auto create", fieldPath);
      return null;
    }
    Field first = values.get(0);
    com.google.cloud.bigquery.Field.Type bqType = DATA_TYPE_MAP.get(first.getType());
    if (bqType != null) {
      return com.google.cloud.bigquery.Field.of(fieldPath, bqType).toBuilder()
          .setMode(Mode.REPEATED).build();
    } else {
      return null;
    }
  }

  private static Map<Field.Type, com.google.cloud.bigquery.Field.Type> DATA_TYPE_MAP =
      new HashMap<>();
  static {
    DATA_TYPE_MAP.put(Type.BOOLEAN, com.google.cloud.bigquery.Field.Type.bool());
    DATA_TYPE_MAP.put(Type.STRING, com.google.cloud.bigquery.Field.Type.string());
    DATA_TYPE_MAP.put(Type.SHORT, com.google.cloud.bigquery.Field.Type.integer());
    DATA_TYPE_MAP.put(Type.LONG, com.google.cloud.bigquery.Field.Type.integer());
    DATA_TYPE_MAP.put(Type.INTEGER, com.google.cloud.bigquery.Field.Type.integer());
    DATA_TYPE_MAP.put(Type.DATE, com.google.cloud.bigquery.Field.Type.date());
    DATA_TYPE_MAP.put(Type.TIME, com.google.cloud.bigquery.Field.Type.time());
    DATA_TYPE_MAP.put(Type.DATETIME, com.google.cloud.bigquery.Field.Type.timestamp());
    DATA_TYPE_MAP.put(Type.FLOAT, com.google.cloud.bigquery.Field.Type.floatingPoint());
    DATA_TYPE_MAP.put(Type.DOUBLE, com.google.cloud.bigquery.Field.Type.floatingPoint());
  }

  class Result {
    private boolean result;
    private String message;
    private List<com.google.cloud.bigquery.Field> fields;

    Result() {
      result = true;
    }

    Result(List<com.google.cloud.bigquery.Field> fields) {
      this();
      this.fields = fields;
    }

    Result(boolean result, String message) {
      this.result = result;
      this.message = message;
    }
  }

  class ErrorRecord {
    private Record record;
    private String messages;
    private String reasons;

    public ErrorRecord(Record record, String messages, String reasons) {
      this.record = record;
      this.messages = messages;
      this.reasons = reasons;
    }
  }
}
