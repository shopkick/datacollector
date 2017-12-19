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
	    required = false,
	    type = ConfigDef.Type.BOOLEAN,
	    defaultValue = "true",
	    label = "Auto Add Table",
	    description = "If enabled, dynamically adds missing table in bigquery",
	    displayPosition = 40,
	    group = "BIGQUERY"
	)
	public boolean autoAddTable;	
	  
    @ConfigDef(
        required = true,
        type = ConfigDef.Type.MODEL,
        label = "Invalid column Handling",
        description = "Handling method when an invalid column is detected",
        defaultValue = "AUTO_ADD_COLUMNS",
        displayPosition = 50,
        group = "BIGQUERY"
    )
    @ValueChooserModel(InvalidColumnHandlerValues.class)
    public InvalidColumnHandler invalidColumnHandler = InvalidColumnHandler.AUTO_ADD_COLUMNS;
    
    @ConfigDef(
        required =  true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "-1",
        label = "Table Cache size",
        description = "Configures the cache size for storing TableId entries." +
            " Use -1 for unlimited number of tableId entries in the cache.",
        displayPosition = 60,
        group = "BIGQUERY",
        min = -1,
        max = Integer.MAX_VALUE
    )
    public int maxCacheSize = -1;
    
    @ConfigDef(
        required =  true,
        type = ConfigDef.Type.NUMBER,
        defaultValue = "2",
        label = "MaxWaitTime for Retries(min)",
        description = "Sometimes bigquery insert API takes time to refresh" +
            " the new schema after a table update, typically 2 mins.",
        displayPosition = 70,
        group = "BIGQUERY",
        min = 0,
        max = 1440,
        dependsOn = "invalidColumnHandler",
        triggeredByValue = {"AUTO_ADD_COLUMNS"}        
    )
    public int maxWaitTimeForInsertMins = 2;      

    @ConfigDefBean(groups = "CREDENTIALS")
    public GoogleCloudCredentialsConfig credentials = new GoogleCloudCredentialsConfig();
}
