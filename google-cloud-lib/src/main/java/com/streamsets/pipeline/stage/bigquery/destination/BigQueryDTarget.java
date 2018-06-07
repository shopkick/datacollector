/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.stage.bigquery.destination;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;

@StageDef(
    version = 2,
    label = "Google BigQuery",
    description = "Streams data into Google Big Query",
    icon="bigquery.png",
    producesEvents = false,
    upgrader = BigQueryTargetUpgrader.class,
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Destinations/BigQuery.html#task_gxn_dsk_dbb"
)
@ConfigGroups(Groups.class)
public class BigQueryDTarget extends DTarget {

  @ConfigDefBean
  public BigQueryTargetConfig conf;

  @Override
  protected Target createTarget() {
    return new BigQueryTarget(conf);
  }
}
