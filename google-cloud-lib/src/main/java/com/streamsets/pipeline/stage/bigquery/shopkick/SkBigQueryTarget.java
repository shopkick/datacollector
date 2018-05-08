package com.streamsets.pipeline.stage.bigquery.shopkick;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
  private static final int INITIAL_ELAPSED_SEC = 0;
  private int backoffMaxWaitTimeSec;
  private boolean retryForInsertErrors;
  private boolean autoAddColumns;
  private boolean autoAddTable;
  private boolean errorHandlingMode;

  public SkBigQueryTarget(SkBigQueryTargetConfig conf, BigQueryTargetConfig bqConf) {
    super(bqConf);

    if (conf.modeHandler == ModeHandler.SCHEMA_DRIFT) {
      this.autoAddColumns = InvalidColumnHandler.AUTO_ADD_COLUMNS.equals(conf.invalidColumnHandler)
          && !bqConf.ignoreInvalidColumn;
      this.retryForInsertErrors = conf.autoAddRetryHandler == AutoAddColRetryHandler.BLOCKING;
      this.autoAddTable = conf.autoAddTable;
      this.backoffMaxWaitTimeSec = conf.maxWaitTimeForInsertMins * 60;
    } else if (conf.modeHandler == ModeHandler.ERROR_HANLDER) {
      this.errorHandlingMode = true;
      this.retryForInsertErrors = true;
      this.backoffMaxWaitTimeSec = conf.maxWaitTimeForRetryMins * 60;
    }
  }

  @Override
  protected void handleTableNotFound(Record record, String datasetName, String tableName,
      Map<TableId, List<Record>> tableIdToRecords) {
    if (!autoAddTable) {
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
      LOG.warn("Exception in big query auto create table {}.{}", datasetName, tableName, e);
      super.handleTableNotFound(record, datasetName, Errors.BIGQUERY_18, tableName, null);
      return;
    }
    if (result == null || !result.result) {
      setErrorAttribute(ERR_BQ_AUTO_CREATE_TABLE, record, result.message);
      LOG.debug("Auto create failed for table {}.{}. Message: {}", datasetName, tableName,
          result.message);
      super.handleTableNotFound(record, datasetName, Errors.BIGQUERY_18, tableName, null);
    }
  }

  @Override
  protected void handleInsertErrors(TableId tableId, ELVars elVars,
      Map<Long, Record> requestIndexToRecords, InsertAllRequest request,
      InsertAllResponse response) {
    if (errorHandlingMode) {
      handlerRetriesForErrorHandler(tableId, elVars, requestIndexToRecords, request, response);
      return;
    }

    if (!autoAddColumns) {
      super.reportErrors(tableId, requestIndexToRecords, response);
      return;
    }

    List<Record> retry = new ArrayList<>();
    List<ErrorRecord> stopped = new ArrayList<>();
    List<ErrorRecord> missingCols = new ArrayList<>();

    bucketizeErrors(tableId, requestIndexToRecords, response, stopped, missingCols, true);

    if (!missingCols.isEmpty()) {
      addMissingColumnsInBigQuery(tableId, retry, missingCols);
    }

    if (!retry.isEmpty()) {
      retryBatchForSchemaDrift(tableId, elVars, retry, stopped);
    } else {
      if (!stopped.isEmpty()) {
        stopped.forEach(r -> super.addToError(r.record, r.messages, r.reasons));
      }
    }
  }

  private void handlerRetriesForErrorHandler(TableId tableId, ELVars elVars,
      Map<Long, Record> requestIndexToRecords, InsertAllRequest request,
      InsertAllResponse response) {
    if (isTimeoutError(requestIndexToRecords, response)) {
      retryBatchForTimeoutError(tableId, requestIndexToRecords, request, response,
          INITIAL_SLEEP_SEC, INITIAL_ELAPSED_SEC);
    } else if (isErrorDueToSchemaDrift(tableId, requestIndexToRecords, response)) {
      retryBatchForSchemaDrift(requestIndexToRecords, elVars, tableId, request, INITIAL_SLEEP_SEC,
          INITIAL_ELAPSED_SEC);
    } else {
      super.reportErrors(tableId, requestIndexToRecords, response);
    }
  }

  private boolean isErrorDueToSchemaDrift(TableId tableId, Map<Long, Record> requestIndexToRecords,
      InsertAllResponse response) {
    Map<Long, List<BigQueryError>> insertErrors = response.getInsertErrors();
    List<Record> newFieldsRecords = new ArrayList<>();
    for (Entry<Long, List<BigQueryError>> entry : insertErrors.entrySet()) {
      List<BigQueryError> errors = entry.getValue();
      String messages = errorsToString(errors, BigQueryError::getMessage);
      String reasons = errorsToString(errors, BigQueryError::getReason);
      if (REASON_INVALID.equalsIgnoreCase(reasons) && NO_SUCH_FIELD.equalsIgnoreCase(messages)) {
        newFieldsRecords.add(requestIndexToRecords.get(entry.getKey()));
      } else if (!REASON_STOPPED.equalsIgnoreCase(reasons)) {
        LOG.debug(
            "Error Handling retry error: Cannot retry batch due to unknown errors: " + reasons);
        return false;
      }
    }
    if (newFieldsRecords.isEmpty()) {
      return false;
    }

    String tableName = tableId.getTable();
    if (isPartitioned(tableName)) {
      tableName = extractTableName(tableName);
    }

    // Table ID without partition suffix
    TableId tableIdOnly = TableId.of(tableId.getDataset(), tableName);
    Table table = bigQuery.getTable(tableIdOnly);
    return newFieldsRecords.stream()
        .allMatch(record -> getAllColumns(table, record).fields.isEmpty());
  }

  private void retryBatchForTimeoutError(TableId tableId, Map<Long, Record> requestIndexToRecords,
      InsertAllRequest request, InsertAllResponse response, int sleepTimeSec, int elapsedSec) {

    InsertAllResponse retryResponse = null;
    boolean retry = false;
    BigQueryException bigQueryException = null;
    try {
      retryResponse = bigQuery.insertAll(request);
      if (retryResponse.hasErrors()) {
        if (!isTimeoutError(requestIndexToRecords, retryResponse)) {
          LOG.warn("<timeOutRetry> Not a timeout issue, sending batch to error");
          super.reportErrors(tableId, requestIndexToRecords, retryResponse);
          return;
        } else {
          retry = true;
        }
      }
    } catch (BigQueryException e) {
      bigQueryException = e;
      if (e.isRetryable()) {
        retry = true;
      } else {
        LOG.warn("<timeOutRetry> Non-retryable exception, sending batch to error");
        LOG.error(Errors.BIGQUERY_13.getMessage(), e);
        // Put all records to error.
        for (long i = 0; i < request.getRows().size(); i++) {
          Record record = requestIndexToRecords.get(i);
          getContext().toError(record, Errors.BIGQUERY_13, e);
        }
        return;
      }
    }
    if (retry) {
      if (elapsedSec >= backoffMaxWaitTimeSec) {
        LOG.warn("<timeOutRetry> Already elapsed {} seconds, sending batch to error", elapsedSec);
        if (bigQueryException != null) {
          super.handleBigqueryException(requestIndexToRecords, request, bigQueryException);
        } else {
          super.reportErrors(tableId, requestIndexToRecords, retryResponse);
        }
        return;
      }
      sleep(sleepTimeSec);
      retryBatchForTimeoutError(tableId, requestIndexToRecords, request, retryResponse,
          sleepTimeSec + sleepTimeSec, elapsedSec + sleepTimeSec);
    } else {
      LOG.debug("<timeOutRetry> Batch for table {} with size {} retried successfully", tableId,
          requestIndexToRecords.size());
    }
  }

  private boolean isTimeoutError(Map<Long, Record> requestIndexToRecords,
      InsertAllResponse response) {
    boolean isTimeOut = false;
    if (response != null) {
      Map<Long, List<BigQueryError>> insertErrors = response.getInsertErrors();
      Set<Entry<Long, List<BigQueryError>>> entrySet = insertErrors.entrySet();
      for (Entry<Long, List<BigQueryError>> entry : entrySet) {
        Long requestIdx = entry.getKey();
        List<BigQueryError> errors = entry.getValue();
        Record record = requestIndexToRecords.get(requestIdx);
        String messages = errorsToString(errors, BigQueryError::getMessage);
        String reasons = errorsToString(errors, BigQueryError::getReason);
        String locations = errorsToString(errors, BigQueryError::getLocation);
        LOG.debug(
            "<ERROR_HANDLER_RETRY> Handling Error retry for record {}, Reasons : {}, Messages : {}, Locations: {}",
            record.getHeader().getSourceId(), reasons, messages, locations);
        if (!REASON_TIMEOUT.equalsIgnoreCase(reasons)
            && !REASON_STOPPED.equalsIgnoreCase(reasons)) {
          LOG.debug("Unknown Reasons:Messages {}:{} to retry for record: {}", reasons, messages,
              record.getHeader().getSourceId());
          return false;
        } else if (!isTimeOut) {
          isTimeOut = true;
        }
      }
    }
    return isTimeOut;
  }

  @Override
  protected void handleInsertError(TableId tableId, Record record, String messages,
      String reasons) {
    setTableIdAttributes(tableId, record);
    if (reasons != null && reasons.contains(REASON_TIMEOUT) && !errorHandlingMode) {
      addRetryFlagInHeader(tableId, -1, record);
    }
    super.addToError(record, messages, reasons);
  }

  private void setTableIdAttributes(TableId tableId, Record record) {
    record.getHeader().setAttribute(BQ_TABLE_ID_DATASET, tableId.getDataset());
    record.getHeader().setAttribute(BQ_TABLE_ID_TABLE, tableId.getTable());
  }

  private void retryBatchForSchemaDrift(TableId tableId, ELVars elVars, List<Record> retry,
      List<ErrorRecord> stopped) {
    Map<Long, Record> requestIndexToRecords = new HashMap<>();
    final AtomicLong index = new AtomicLong(0);
    InsertAllRequest.Builder insertAllRequestBuilder = InsertAllRequest.newBuilder(tableId);
    insertAllRequestBuilder.setIgnoreUnknownValues(false);
    insertAllRequestBuilder.setSkipInvalidRows(false);
    addToInsertRequest(tableId, elVars, requestIndexToRecords, index, retry,
        insertAllRequestBuilder);

    if (stopped != null && !stopped.isEmpty()) {
      addToInsertRequest(tableId, elVars, requestIndexToRecords, index,
          stopped.stream().map(e -> e.record).collect(Collectors.toList()),
          insertAllRequestBuilder);
    }

    InsertAllRequest req = insertAllRequestBuilder.build();

    retryBatchForSchemaDrift(requestIndexToRecords, elVars, tableId, req, INITIAL_SLEEP_SEC,
        INITIAL_ELAPSED_SEC);
  }

  private void retryBatchForSchemaDrift(Map<Long, Record> requestIndexToRecords, ELVars elVars,
      TableId tableId, InsertAllRequest request, int sleepTimeSec, int elapsedSec) {

    if (elapsedSec > backoffMaxWaitTimeSec) {
      LOG.info("Last retry for sending batch. Already Elapsed: {} Secs", elapsedSec);
      addRetryFlagInHeaders(tableId, requestIndexToRecords, -1);
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
          bucketizeErrors(tableId, requestIndexToRecords, response, stopped, missingCols, false);
          if (missingCols.size() > 0
              && (requestIndexToRecords.size() == (stopped.size() + missingCols.size()))) {
            if (!retryForInsertErrors) {
              LOG.debug("Auto Add Col Insert Error Retry disabled, sending batch to error handler");
              addRetryFlagInHeaders(tableId, requestIndexToRecords, -1);
              super.reportErrors(tableId, requestIndexToRecords, response);
              return;
            }
            sleep(sleepTimeSec);
            retryBatchForSchemaDrift(requestIndexToRecords, elVars, tableId, request,
                sleepTimeSec + sleepTimeSec, elapsedSec + sleepTimeSec);
          } else {
            LOG.warn("Cannot insert after auto add, found unknown errors");
            super.reportErrors(tableId, requestIndexToRecords, response);
            return;
          }
        } else {
          LOG.debug("insertAllWithRetries Success");
        }
      } catch (BigQueryException e) {
        LOG.error(Errors.BIGQUERY_13.getMessage(), e);
        addRetryFlagIfApplicable(tableId, requestIndexToRecords, e);
        // Put all records to error.
        for (long i = 0; i < request.getRows().size(); i++) {
          Record record = requestIndexToRecords.get(i);
          getContext().toError(record, Errors.BIGQUERY_13, e);
        }
      }
    }
  }

  private void addMissingColumnsInBigQuery(TableId tableId, List<Record> retry,
      List<ErrorRecord> missingCols) {
    missingCols.forEach(err -> {
      Result added = addMissingColumnsInBigQuery(tableId, err.record, 0);
      if (added.result) {
        retry.add(err.record);
      } else {
        LOG.debug(added.message);
        setErrorAttribute(ERR_BQ_AUTO_ADD_COLUMNS, err.record, added.message);
        super.addToError(err.record, err.messages, err.reasons);
      }
    });
  }


  private void addRetryFlagInHeaders(TableId tableId, Map<Long, Record> requestIndexToRecords,
      int errorCode) {

    if (errorHandlingMode) {
      // No need to change the previous retry flag in case of error handling
      return;
    }

    Iterator<Entry<Long, Record>> iterator = requestIndexToRecords.entrySet().iterator();
    while (iterator.hasNext()) {
      Record record = iterator.next().getValue();
      addRetryFlagInHeader(tableId, errorCode, record);
    }
  }

  private void addRetryFlagInHeader(TableId tableId, int errorCode, Record record) {
    setErrorAttribute(ERR_ACTION, record, RETRY);
    if (errorCode != -1) {
      setErrorAttribute(ERR_BQ_ERROR_CODE, record, Integer.toString(errorCode));
    }
  }

  @Override
  protected void handleBigqueryException(Map<Long, Record> requestIndexToRecords,
      InsertAllRequest request, BigQueryException e) {
    
    if (requestIndexToRecords != null) {
      TableId tableId = request.getTable();
      requestIndexToRecords.values().forEach(record -> {
        setTableIdAttributes(tableId, record);
      });
    }
    
    if (e.isRetryable()) {
      if (errorHandlingMode) {
        retryBatchForTimeoutError(request.getTable(), requestIndexToRecords, request, null,
            INITIAL_SLEEP_SEC, INITIAL_ELAPSED_SEC);
      } else {
        addRetryFlagInHeaders(request.getTable(), requestIndexToRecords, e.getCode());
        super.handleBigqueryException(requestIndexToRecords, request, e);
      }
    } else {
      super.handleBigqueryException(requestIndexToRecords, request, e);
    }
  }

  private void addRetryFlagIfApplicable(TableId tableId, Map<Long, Record> requestIndexToRecords,
      BigQueryException e) {
    if (e.isRetryable()) {
      addRetryFlagInHeaders(tableId, requestIndexToRecords, e.getCode());
    }
  }

  private void sleep(int sleepTimeSec) {
    try {
      LOG.info("Waiting {} seconds before retrying the batch", sleepTimeSec);
      TimeUnit.SECONDS.sleep(sleepTimeSec);
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
    }
  }

  private void addToInsertRequest(TableId tableId, ELVars elVars,
      Map<Long, Record> requestIndexToRecords, AtomicLong index, List<Record> retry,
      InsertAllRequest.Builder insertAllRequestBuilder) {
    retry.forEach(record -> {
      try {
        String insertId = getInsertIdForRecord(elVars, record);
        Map<String, ?> rowContent = convertToRowObjectFromRecord(record);
        if (rowContent.isEmpty()) {
          throw new OnRecordErrorException(record, Errors.BIGQUERY_14);
        }
        requestIndexToRecords.put(index.getAndIncrement(), record);
        insertAllRequestBuilder.addRow(insertId, rowContent);
      } catch (OnRecordErrorException e) {
        LOG.error("Error when converting record {} to row, Reason : {} ",
            record.getHeader().getSourceId(), e.getMessage());
        getContext().toError(record, e.getErrorCode(), e.getParams());
      }
    });
  }

  protected boolean includeField(Field field) {
    if (super.includeField(field)) {
      Type type = field.getType();
      if (type == Type.LIST) {
        List<Field> value = field.getValueAsList();
        return value != null && !value.isEmpty();
      } else if (type == Type.MAP || type == Type.LIST_MAP) {
        Map<String, Field> value = field.getValueAsMap();
        if (value == null || value.isEmpty()) {
          return false;
        }
        Iterator<Entry<String, Field>> iterator = value.entrySet().iterator();
        while (iterator.hasNext()) {
          Field subVal = iterator.next().getValue();
          if (subVal == null) {
            return false;
          }
          if (!includeField(subVal)) {
            return false;
          }
        }
      }
    } else {
      return false;
    }
    return true;
  }

  private void bucketizeErrors(TableId tableId, Map<Long, Record> requestIndexToRecords,
      InsertAllResponse response, List<ErrorRecord> stopped, List<ErrorRecord> missingCols,
      boolean reportError) {
    response.getInsertErrors().forEach((requestIdx, errors) -> {
      Record record = requestIndexToRecords.get(requestIdx);
      String messages = errorsToString(errors, BigQueryError::getMessage);
      String reasons = errorsToString(errors, BigQueryError::getReason);
      String locations = errorsToString(errors, BigQueryError::getLocation);
      LOG.debug(
          "<Auto_add_column> Handling Error when inserting record {}, Reasons : {}, Messages : {}, Locations: {}",
          record.getHeader().getSourceId(), reasons, messages, locations);
      if (REASON_STOPPED.equalsIgnoreCase(reasons)) {
        stopped.add(new ErrorRecord(record, messages, reasons));
      } else if (REASON_INVALID.equalsIgnoreCase(reasons)
          && NO_SUCH_FIELD.equalsIgnoreCase(messages)) {
        missingCols.add(new ErrorRecord(record, messages, reasons));
      } else if (reportError) {
        LOG.debug("Auto create error: Cannot bucketize unknown errors: " + reasons);
        handleInsertError(tableId, record, messages, reasons);
      }
    });
  }

  private String errorsToString(List<BigQueryError> errors,
      Function<? super BigQueryError, ? extends String> function) {
    return COMMA_JOINER.join(errors.stream().map(function).collect(Collectors.toList()));
  }

  private Result addMissingColumnsInBigQuery(TableId tableId, Record record, int retry) {

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
      return updateTable(tableId, record, retry, tableIdOnly, table, result.fields);
    } else {
      LOG.debug("No additional fields found to update the table: {}", tableIdOnly);
    }

    return new Result();
  }

  private Result updateTable(TableId tableId, Record record, int retry, TableId tableIdOnly,
      Table table, List<com.google.cloud.bigquery.Field> fields) {
    Schema schema = Schema.of(fields);
    TableInfo tableInfo =
        table.toBuilder().setDefinition(StandardTableDefinition.of(schema)).build();

    Table update = null;

    try {
      update = bigQuery.update(tableInfo);
      LOG.info("Table updated with additional fields: " + update);
    } catch (BigQueryException e) {
      if (e.getCode() == ERR_CODE_BAD_REQUEST) {
        BigQueryError error = e.getError();
        if (error != null && error.getMessage() != null
            && error.getMessage().endsWith(OLD_SCHEMA_ERROR_SUFFIX)) {
          if (retry >= MAX_RETRIES) {
            return new Result(false, "Table update error - Missing schema - retries exhausted");
          }
          return addMissingColumnsInBigQuery(tableIdOnly, record, retry + 1);
        }
      }
      if (e.isRetryable()) {
        addRetryFlagInHeader(tableId, e.getCode(), record);
      }
      throw e;
    }

    boolean updateDone = isTableUpdated(update, tableId, fields);

    if (!updateDone) {
      String errMsg = String.format("Update not getting reflected in table %s", tableId);
      LOG.warn(errMsg);
      return new Result(false, errMsg);
    }

    return new Result();
  }

  private boolean isTableUpdated(Table updateResponse, TableId tableId,
      List<com.google.cloud.bigquery.Field> fields) {
    int retryCount = 0;

    boolean updateDone = checkTableHasFields(updateResponse, fields);
    while (!updateDone && retryCount < ADD_COLS_RETRIES) {
      try {
        LOG.debug("Sleeping for {} seconds for checking whether table {} updated",
            RETRY_SLEEP_TIME_MS, tableId);
        TimeUnit.MILLISECONDS.sleep(RETRY_SLEEP_TIME_MS);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted", e);
      }
      updateDone = checkTableHasFields(tableId, fields);
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

    boolean containsAll = tableFields.containsAll(allFields);
    if (!containsAll) {
      for (com.google.cloud.bigquery.Field field : allFields) {
        if (!tableFields.contains(field)) {
          LOG.warn("Field {} expected in big query, but not found", field);
        }
      }
    }
    return containsAll;
  }

  private Result getAllColumns(Table table, Record record) {
    List<com.google.cloud.bigquery.Field> bqFields = table.getDefinition().getSchema().getFields();
    List<com.google.cloud.bigquery.Field> additional = new ArrayList<>();

    Map<String, com.google.cloud.bigquery.Field> bqFieldMap = new HashMap<>();

    if (bqFields != null) {
      bqFields.forEach(f -> bqFieldMap.put(f.getName(), f));
    }

    Set<String> ssFieldPaths = getValidFieldPaths(record.getEscapedFieldPaths());

    if (ssFieldPaths == null) {
      return new Result(false,
          "Auto create column failed. No valid field path exists in the record " + record);
    }

    for (String ssFieldPath : ssFieldPaths) {
      String ssFieldName = getFieldNameFromPath(ssFieldPath);

      if (!bqFieldMap.containsKey(ssFieldName)) {
        LOG.debug("Found new field: [{}] in record which is not in bigquery, detecting schema",
            ssFieldName);
        Field ssField = record.get(ssFieldPath);
        com.google.cloud.bigquery.Field bqFieldNew = convertSsToBqField(ssField, ssFieldName);

        if (bqFieldNew == null) {
          LOG.debug("Cannot determine schema for field: {}, Type: {}, ignoring", ssFieldPath,
              ssField.getType());
          continue;
        }

        bqFieldNew = bqFieldNew.toBuilder().setDescription(getDescription()).build();
        additional.add(bqFieldNew);
      }
    }

    if (!additional.isEmpty()) {
      LOG.info("Additional fields found in the record: {}, will try to update table: {}",
          additional, table.getTableId());
      additional.addAll(bqFields);
      return new Result(additional);
    }

    return new Result();
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
    createTable(tableId, fieldsResult.fields, record);
    return new Result();
  }

  private Result convertSsToBqFields(Record record) {
    List<com.google.cloud.bigquery.Field> fields = new ArrayList<>();
    Set<String> fieldPaths = record.getEscapedFieldPaths();

    Set<String> validPaths = getValidFieldPaths(fieldPaths);

    if (validPaths == null) {
      return new Result(false,
          "Auto create failed. No valid field path exists in the record " + record);
    }

    for (String fieldPath : validPaths) {
      Field field = record.get(fieldPath);
      LOG.debug("Converting ss field {} with type {} to bq field", fieldPath, field.getType());
      String fieldName = getFieldNameFromPath(fieldPath);
      com.google.cloud.bigquery.Field bqField = convertSsToBqField(field, fieldName);
      if (bqField == null) {
        LOG.debug("Cannot determine schema for field: {}, Type: {}, ignoring", fieldPath,
            field.getType());
        continue;
      }
      fields.add(bqField);
    }

    if (!fields.isEmpty()) {
      return new Result(fields);
    } else {
      return new Result(false,
          "Auto create failed. Cannot determine schema for any field:" + fieldPaths);
    }
  }

  private String getFieldNameFromPath(String fieldPath) {
    return fieldPath.replaceFirst(FORWARD_SLASH, EMPTY);
  }

  private Set<String> getValidFieldPaths(Set<String> fieldPaths) {
    if (fieldPaths == null || fieldPaths.isEmpty()) {
      return null;
    }

    return fieldPaths.parallelStream().filter(e -> isValidFieldPath(e)).collect(Collectors.toSet());
  }

  private void createTable(TableId tableId, List<com.google.cloud.bigquery.Field> fields,
      Record record) {
    TableInfo tableInfo = buildTableSchema(tableId, fields);
    Table table = null;

    try {
      table = bigQuery.create(tableInfo);
      LOG.info("Table {} not found, created", tableInfo.getTableId());
    } catch (BigQueryException e) {
      if (e.getCode() == ERR_CODE_DUPLICATE) {
        LOG.info("Table {} already created, not trying", tableInfo.getTableId());
        table = bigQuery.getTable(tableInfo.getTableId());
      } else {
        if (e.isRetryable()) {
          addRetryFlagInHeader(tableId, e.getCode(), record);
        }
        throw e;
      }
    }
    LOG.debug("Table details {}", table);
  }

  private TableInfo buildTableSchema(TableId tableId,
      List<com.google.cloud.bigquery.Field> fields) {
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
    return tableInfo.toBuilder().setDescription(getDescription()).build();
  }

  private static String getDescription() {
    return AUTO_ADDED_BY_STREAMSETS + Instant.now();
  }

  protected String extractTableName(String tableName) {
    return tableName.substring(0, tableName.length() - PARTITION_DATE_SUFFIX_LEN);
  }

  private boolean isPartitioned(String table) {
    return PARTITION_PATTERN.matcher(table).find();
  }

  private boolean isValidFieldPath(String fieldPath) {
    if (fieldPath != null && fieldPath.trim().isEmpty()) {
      return false;
    }

    if (fieldPath.charAt(0) != FORWARD_SLASH_CHAR) {
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
  private com.google.cloud.bigquery.Field convertSsToBqField(Field field, String fieldName) {
    if(field.getValue() == null) {
      LOG.debug("Field {} value is null, unable to determine datatype", fieldName);
      return null;
    }
    
    if (Type.LIST.equals(field.getType())) {
      return getFieldForList(field, fieldName);
    }

    if (Type.LIST_MAP.equals(field.getType()) || Type.MAP.equals(field.getType())) {
      return getFieldForMap(field, fieldName);
    }

    com.google.cloud.bigquery.Field.Type bqFieldType = DATA_TYPE_MAP.get(field.getType());
    if (bqFieldType != null) {
      return com.google.cloud.bigquery.Field.newBuilder(fieldName, bqFieldType)
          .setMode(Mode.NULLABLE).build();
    }
    return null;
  }

  /**
   * Convert Stremsets List field to Bigquery Repeated field with corresponding datatype
   */
  private com.google.cloud.bigquery.Field getFieldForList(Field field, String fieldName) {
    List<Field> values = field.getValueAsList();
    if (values == null || values.isEmpty()) {
      LOG.debug("List: {} empty, cannot determine data type. Cannot auto create", fieldName);
      return null;
    }

    Field first = values.get(0);
    com.google.cloud.bigquery.Field.Type bqType = null;
    Type firstType = first.getType();
    if (Type.LIST_MAP.equals(firstType) || Type.MAP.equals(firstType)) {
      bqType = getBqTypeForMap(first, fieldName + "[0]");
      if (bqType == null) {
        return null;
      }
    } else if (DATA_TYPE_MAP.containsKey(firstType)) {
      bqType = DATA_TYPE_MAP.get(firstType);
    } else {
      return null;
    }

    return com.google.cloud.bigquery.Field.of(fieldName, bqType).toBuilder().setMode(Mode.REPEATED)
        .build();
  }

  /**
   * Convert Stremsets ListMap field to Bigquery Record field with corresponding datatypes for each
   * field in record
   */
  private com.google.cloud.bigquery.Field getFieldForMap(Field ssField, String fieldName) {
    com.google.cloud.bigquery.Field.Type bqType = getBqTypeForMap(ssField, fieldName);
    if (bqType == null) {
      return null;
    }
    return com.google.cloud.bigquery.Field.of(fieldName, bqType).toBuilder().build();
  }

  private com.google.cloud.bigquery.Field.Type getBqTypeForMap(Field ssField, String fieldName) {
    Map<String, Field> value = ssField.getValueAsMap();

    if (value == null || value.isEmpty()) {
      LOG.warn("Cannot auto add column {} for type {} since the value is empty", fieldName,
          ssField.getType());
      return null;
    }

    List<com.google.cloud.bigquery.Field> fields = new ArrayList<>();
    Iterator<Entry<String, Field>> iterator = value.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, Field> next = iterator.next();
      String subFieldName = next.getKey();
      Field subField = next.getValue();
      com.google.cloud.bigquery.Field bqField = convertSsToBqField(subField, subFieldName);
      if (bqField == null) {
        LOG.debug("{}: {} empty, cannot determine data type for Subfield", subField.getType(),
            subFieldName);
        return null;
      }
      fields.add(bqField);
    }
    return com.google.cloud.bigquery.Field.Type.record(fields);
  }

  class Result {
    private boolean result;
    private String message;
    private List<com.google.cloud.bigquery.Field> fields = new ArrayList<>();

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

  // ALL CONSTANTS ARE DEFINED BELOW

  private static final Map<Field.Type, com.google.cloud.bigquery.Field.Type> DATA_TYPE_MAP =
      new HashMap<>();

  static {
    DATA_TYPE_MAP.put(Type.BOOLEAN, com.google.cloud.bigquery.Field.Type.bool());
    DATA_TYPE_MAP.put(Type.BYTE_ARRAY, com.google.cloud.bigquery.Field.Type.bytes());
    DATA_TYPE_MAP.put(Type.STRING, com.google.cloud.bigquery.Field.Type.string());
    DATA_TYPE_MAP.put(Type.SHORT, com.google.cloud.bigquery.Field.Type.integer());
    DATA_TYPE_MAP.put(Type.LONG, com.google.cloud.bigquery.Field.Type.integer());
    DATA_TYPE_MAP.put(Type.INTEGER, com.google.cloud.bigquery.Field.Type.integer());
    DATA_TYPE_MAP.put(Type.DATE, com.google.cloud.bigquery.Field.Type.date());
    DATA_TYPE_MAP.put(Type.TIME, com.google.cloud.bigquery.Field.Type.time());
    DATA_TYPE_MAP.put(Type.DATETIME, com.google.cloud.bigquery.Field.Type.timestamp());
    DATA_TYPE_MAP.put(Type.FLOAT, com.google.cloud.bigquery.Field.Type.floatingPoint());
    DATA_TYPE_MAP.put(Type.DOUBLE, com.google.cloud.bigquery.Field.Type.floatingPoint());

    // List and Map types are handled separately
    // DATA_TYPE_MAP.put(Type.LIST_MAP, com.google.cloud.bigquery.Field.Type.record());
    // DATA_TYPE_MAP.put(Type.LIST, com.google.cloud.bigquery.Field.Type.<repeated_type>));
  }

  private static final String BQ_TABLE_ID_TABLE = "BQ_TABLE_ID_TABLE";
  private static final String BQ_TABLE_ID_DATASET = "BQ_TABLE_ID_DATASET";
  private static final String EMPTY = "";
  private static final String AUTO_ADDED_BY_STREAMSETS = "auto added by streamsets at ";
  private static final String OLD_SCHEMA_ERROR_SUFFIX = "missing in new schema";
  private static final int ERR_CODE_BAD_REQUEST = 400;
  private static final int ERR_CODE_DUPLICATE = 409;
  private static final int INITIAL_SLEEP_SEC = 1;
  private static final int RETRY_SLEEP_TIME_MS = 500;
  private static final int MAX_RETRIES = 3;
  private static final String REASON_STOPPED = "stopped";
  private static final String REASON_INVALID = "invalid";
  private static final String REASON_TIMEOUT = "timeout";
  private static final String NO_SUCH_FIELD = "no such field.";
  private static final String RETRY = "retry";

  private static final int PARTITION_DATE_SUFFIX_LEN = 9;
  private static final char ARRAY_START_CHAR = '[';
  private static final char FORWARD_SLASH_CHAR = '/';
  private static final String FORWARD_SLASH = FORWARD_SLASH_CHAR + EMPTY;
  private static final String ERR_BQ_AUTO_CREATE_TABLE = "ERR_BQ_AUTO_CREATE_TABLE";
  private static final String ERR_BQ_AUTO_ADD_COLUMNS = "ERR_BQ_AUTO_ADD_COLUMNS";
  private static final String ERR_ACTION = "ERR_ACTION";
  private static final String ERR_BQ_ERROR_CODE = "ERR_BIGQUERY_ERROR_CODE";
  private static final Logger LOG = LoggerFactory.getLogger(SkBigQueryTarget.class);

  private static final Pattern PARTITION_PATTERN = Pattern.compile("\\$\\d{4}\\d{2}\\d{2}$");
  private static final int ADD_COLS_RETRIES = 10;


}
