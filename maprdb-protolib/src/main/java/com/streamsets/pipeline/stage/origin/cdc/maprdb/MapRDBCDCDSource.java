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
package com.streamsets.pipeline.stage.origin.cdc.maprdb;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.ExecutionMode;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.PushSource;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DPushSource;

@StageDef(
    version = 1,
    label = "MapR DB CDC Consumer",
    description = "Reads MapR DB CDC data from MapR Streams",
    execution = ExecutionMode.STANDALONE,
    icon = "mapr_db.png",
    recordsByRef = true,
    upgrader = MapRDBCDCSourceUpgrader.class,
    onlineHelpRefUrl = "index.html#datacollector/UserGuide/Origins/MapRdbCDC.html#task_mmx_zvm_pbb"
)
@ConfigGroups(Groups.class)
@GenerateResourceBundle
public class MapRDBCDCDSource extends DPushSource {
  @ConfigDefBean
  public MapRDBCDCBeanConfig conf;

  @Override
  protected PushSource createPushSource() {
    return new MapRDBCDCSource(conf, new MapRDBCDCKafkaConsumerFactory());
  }
}
