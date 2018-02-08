package com.streamsets.pipeline.stage.bigquery.shopkick;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class InvalidColumnHandlerValues extends BaseEnumChooserValues<InvalidColumnHandler> {
  public InvalidColumnHandlerValues() {
	super(
		InvalidColumnHandler.AUTO_ADD_COLUMNS, 
		InvalidColumnHandler.ERROR_INVALID_COLUMNS,
		InvalidColumnHandler.IGNORE_INVALID_COLUMNS);
  }
}
