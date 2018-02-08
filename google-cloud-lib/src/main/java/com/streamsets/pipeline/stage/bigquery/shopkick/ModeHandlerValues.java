package com.streamsets.pipeline.stage.bigquery.shopkick;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

public class ModeHandlerValues extends BaseEnumChooserValues<ModeHandler> {
  public ModeHandlerValues() {
	super(
	    ModeHandler.SCHEMA_DRIFT,
	    ModeHandler.ERROR_HANLDER,
	    ModeHandler.DEFAULT
	    );
  }
}
