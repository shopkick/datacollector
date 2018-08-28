package com.streamsets.pipeline.stage.bigquery.shopkick;

/*
 * Copyright 2017 Shopkick Inc.
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

import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.api.Target;
import com.streamsets.pipeline.api.base.configurablestage.DTarget;
import com.streamsets.pipeline.stage.bigquery.destination.BigQueryTargetConfig;
import com.streamsets.pipeline.stage.bigquery.lib.Groups;

@StageDef(
        version = 1,
        label = "SK Google BigQuery",
        description = "Streams data into Google Big Query with auto add table/column capabilities",
        icon="skbigquery.png",
        producesEvents = false,
        onlineHelpRefUrl = "index.html#Destinations/BigQuery.html#task_gxn_dsk_dbb"
)
@ConfigGroups(Groups.class)
public class SkBigQueryDTarget extends DTarget {

    @ConfigDefBean
    public SkBigQueryTargetConfig conf;

    @Override
    protected Target createTarget() {
        BigQueryTargetConfig bqConf = convert(conf);
        return new SkBigQueryTarget(conf, bqConf);
    }

    private BigQueryTargetConfig convert(SkBigQueryTargetConfig conf) {
        BigQueryTargetConfig bqConf = new BigQueryTargetConfig();
        bqConf.tableNameEL = conf.tableNameEL;
        bqConf.datasetEL = conf.datasetEL;
        bqConf.credentials = conf.credentials;
        bqConf.maxCacheSize = conf.maxCacheSize;
        bqConf.rowIdExpression = conf.rowIdExpression;

        switch (conf.modeHandler) {

            case SCHEMA_DRIFT:
                switch (conf.invalidColumnHandler) {
                    case AUTO_ADD_COLUMNS:
                    case ERROR_INVALID_COLUMNS:
                        bqConf.ignoreInvalidColumn = false;
                        break;
                    case IGNORE_INVALID_COLUMNS:
                        bqConf.ignoreInvalidColumn = true;
                        break;
                    default:
                }
                break;
            case ERROR_HANLDER:
                bqConf.ignoreInvalidColumn = false;
                break;
            case DEFAULT:
                bqConf.ignoreInvalidColumn = conf.ignoreInvalidColumn;
                break;
            default:
                break;
        }

        return bqConf;
    }
}