package com.streamsets.pipeline.stage.bigquery.shopkick;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class AutoAddColRetryHandlerValues extends BaseEnumChooserValues<AutoAddColRetryHandler> {
  public AutoAddColRetryHandlerValues() {
	super(
		AutoAddColRetryHandler.BLOCKING, 
		AutoAddColRetryHandler.NON_BLOCKING);
  }
}
