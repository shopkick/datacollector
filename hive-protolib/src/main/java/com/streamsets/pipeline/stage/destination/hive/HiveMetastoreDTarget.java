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
package com.streamsets.pipeline.stage.destination.hive;


import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;

@StageDef(
    version = 2,
    label = "Hive Metastore",
    description = "Updates the Hive Metastore.",
    icon = "hive.png",
    privateClassLoader = true,
    upgrader = HiveMetastoreTargetUpgrader.class,
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Destinations/HiveMetastore.html#task_a4n_1ft_zv",
    producesEvents = true
)
@ConfigGroups(value = Groups.class)
@GenerateResourceBundle
public class HiveMetastoreDTarget extends DTarget {
  @ConfigDefBean
  public HMSTargetConfigBean conf;
  @Override
  protected Target createTarget() {
    return new HiveMetastoreTarget(conf);
  }
}
