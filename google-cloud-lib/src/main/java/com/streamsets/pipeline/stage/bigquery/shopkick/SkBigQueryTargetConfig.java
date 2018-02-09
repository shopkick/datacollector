package com.streamsets.pipeline.stage.bigquery.shopkick;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.stage.lib.GoogleCloudCredentialsConfig;

public class SkBigQueryTargetConfig{
    @ConfigDef(
        required = true,
        label = "Dataset",
        type = ConfigDef.Type.STRING,
        defaultValue = "${record:attribute('dataset')}",
        description = "Dataset name or an expression to obtain the dataset name from the record",
        displayPosition = 10,
        group = "BIGQUERY",
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        elDefs = {RecordEL.class}
    )
    public String datasetEL;

    @ConfigDef(
        required = true,
        label = "Table Name",
        type = ConfigDef.Type.STRING,
        defaultValue = "${record:attribute('table')}",
        description = "Table name or an expression to obtain the table name from the record",
        displayPosition = 20,
        group = "BIGQUERY",
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        elDefs = {RecordEL.class}
    )
    public String tableNameEL;

    @ConfigDef(
        //Not needed, if not configured, its considered a plain insert without row id
        required = false,
        label = "Insert Id Expression",
        type = ConfigDef.Type.STRING,
        description = "Expression for the insertId to insert or update. " +
            "Leave blank to perform an insert for each record",
        displayPosition = 30,
        group = "BIGQUERY",
        evaluation = ConfigDef.Evaluation.EXPLICIT,
        elDefs = {RecordEL.class}
    )
    public String rowIdExpression;
    
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Mode",
        description = "The mode at which the BigQueryTarget will run",
        defaultValue = "SCHEMA_DRIFT",
        displayPosition = 40,
        group = "BIGQUERY"
    )
    @ValueChooserModel(ModeHandlerValues.class)
    public ModeHandler modeHandler = ModeHandler.SCHEMA_DRIFT;    

    @ConfigDef(
	    required = false,
	    type = ConfigDef.Type.BOOLEAN,
	    defaultValue = "true",
	    label = "Auto Add Table",
	    description = "If enabled, dynamically adds missing table in bigquery",
	    displayPosition = 50,
	    group = "BIGQUERY",
        dependsOn = "modeHandler",
        triggeredByValue = {"SCHEMA_DRIFT"}    	    
	)
	public boolean autoAddTable;	
	  
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Invalid column Handling",
        description = "Handling method when an invalid column is detected",
        defaultValue = "AUTO_ADD_COLUMNS",
        displayPosition = 60,
        group = "BIGQUERY",
        dependsOn = "modeHandler",
        triggeredByValue = {"SCHEMA_DRIFT"}        
    )
    @ValueChooserModel(InvalidColumnHandlerValues.class)
    public InvalidColumnHandler invalidColumnHandler = InvalidColumnHandler.AUTO_ADD_COLUMNS;
    
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Auto Add Retry Handling",
        description = "Retry handling when insert call fails after adding columns to a table)",
        defaultValue = "NON_BLOCKING",
        displayPosition = 70,
        group = "BIGQUERY",
        dependsOn = "invalidColumnHandler",
        triggeredByValue = {"AUTO_ADD_COLUMNS"}         
    )
    @ValueChooserModel(AutoAddColRetryHandlerValues.class)
    public AutoAddColRetryHandler autoAddRetryHandler = AutoAddColRetryHandler.NON_BLOCKING;    
    
    @ConfigDef(
        required =  true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "3",
        label = "MaxWaitTime for Retries(min)",
        description = "Maximum backoff retry time(minutes) for insert failures" +
            " due to schema drift.",
        displayPosition = 80,
        group = "BIGQUERY",
        min = 0,
        max = 1440,
        dependsOn = "autoAddRetryHandler",
        triggeredByValue = {"BLOCKING"}        
    )
    public int maxWaitTimeForInsertMins = 3;
    
    @ConfigDef(
        required =  true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "3",
        label = "MaxWaitTime for Retries(min)",
        description = "Maximum backoff retry time(minutes) for insert failures" +
            " due to timeout errors or schema drift.",
        displayPosition = 90,
        group = "BIGQUERY",
        min = 0,
        max = 1440,
        dependsOn = "modeHandler",
        triggeredByValue = {"ERROR_HANLDER"}        
    )
    public int maxWaitTimeForRetryMins = 3;    
    
    @ConfigDef(
        required = false,
        type = ConfigDef.Type.BOOLEAN,
        defaultValue = "true",
        label = "Ignore Invalid Column",
        description = "If enabled, field paths that cannot be mapped to columns will be ignored",
        displayPosition = 100,
        group = "BIGQUERY",
        dependsOn = "modeHandler",
        triggeredByValue = {"DEFAULT"}
    )
    public boolean ignoreInvalidColumn;
    
    @ConfigDef(
        required =  true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "-1",
        label = "Table Cache size",
        description = "Configures the cache size for storing TableId entries." +
            " Use -1 for unlimited number of tableId entries in the cache.",
        displayPosition = 110,
        group = "BIGQUERY",
        min = -1,
        max = Integer.MAX_VALUE
    )
    public int maxCacheSize = -1;
    
    @ConfigDefBean(groups = "CREDENTIALS")
    public GoogleCloudCredentialsConfig credentials = new GoogleCloudCredentialsConfig();
}
