package com.streamsets.pipeline.stage.bigquery.shopkick;

import com.streamsets.pipeline.api.Label;

public enum AutoAddColRetryHandler implements Label {
  BLOCKING("Retry message with a backoff till maxWaitTime(blocking)"),
  NON_BLOCKING("Redirect message to error stream(non-blocking)");

  private final String label;

  AutoAddColRetryHandler(String label) {
    this.label = label;
  }

  @Override
  public String getLabel() {
    return label;
  }
}
