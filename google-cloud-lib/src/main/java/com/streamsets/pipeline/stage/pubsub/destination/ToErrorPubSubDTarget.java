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
package com.streamsets.pipeline.stage.pubsub.destination;

import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.config.DataFormat;

@StageDef(
    version = 1,
    label = "Write to Google Pub Sub",
    description = "Writes error records to Google Pub Sub",
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Pipeline_Configuration/ErrorHandling.html#concept_kgc_l4y_5r"
)
@ErrorStage
@HideConfigs(preconditions = true, onErrorRecord = true, value = {
    "conf.dataFormat",
})
@GenerateResourceBundle
public class ToErrorPubSubDTarget extends PubSubDTarget {
  @Override
  protected Target createTarget() {
    conf.dataFormat = DataFormat.SDC_JSON;
    return new PubSubTarget(conf);
  }
}
