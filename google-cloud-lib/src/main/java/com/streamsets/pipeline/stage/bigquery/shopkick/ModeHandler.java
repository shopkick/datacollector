package com.streamsets.pipeline.stage.bigquery.shopkick;

import com.streamsets.pipeline.api.Label;

public enum ModeHandler implements Label {
  SCHEMA_DRIFT("Schema Drift"),
  ERROR_HANLDER("Error handling"),
  DEFAULT("Default");

  private final String label;

  ModeHandler(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
