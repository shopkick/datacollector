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
package com.streamsets.pipeline.stage.destination.hive.queryexecutor;

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.Executor;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.HideConfigs;
import com.streamsets.pipeline.api.PipelineLifecycleStage;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.base.configurablestage.DExecutor;

/**
 */
@StageDef(
    version = 2,
    label = "Hive Query",
    description = "Executes Hive or Impala queries.",
    icon = "hive-executor.png",
    privateClassLoader = true,
    producesEvents = true,
    upgrader = HiveQueryExecutorUpgrader.class,
    onlineHelpRefUrl ="index.html#datacollector/UserGuide/Executors/HiveQuery.html#task_mgm_4lk_fx"
)
@ConfigGroups(value = Groups.class)
@HideConfigs({
  "config.hiveConfigBean.maxCacheSize"
})
@PipelineLifecycleStage
@GenerateResourceBundle
public class HiveQueryDExecutor extends DExecutor {

  @ConfigDefBean
  public HiveQueryExecutorConfig config;

  @Override
  protected Executor createExecutor() {
    return new HiveQueryExecutor(config);
  }
}
