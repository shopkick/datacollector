package com.streamsets.pipeline.stage.bigquery.shopkick;

import com.streamsets.pipeline.api.Label;

public enum InvalidColumnHandler implements Label {
  AUTO_ADD_COLUMNS("Auto Add Columns"),
  IGNORE_INVALID_COLUMNS("Ignore Invalid Columns"),
  ERROR_INALID_COLUMNS("Error on detecting invalid columns");

  private final String label;

  InvalidColumnHandler(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
