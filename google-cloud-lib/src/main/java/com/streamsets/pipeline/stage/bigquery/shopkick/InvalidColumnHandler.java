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

import com.streamsets.pipeline.api.Label;

public enum InvalidColumnHandler implements Label {
    AUTO_ADD_COLUMNS("Auto Add Columns"),
    IGNORE_INVALID_COLUMNS("Ignore Invalid Columns"),
    ERROR_INVALID_COLUMNS("Error on detecting invalid columns");

    private final String label;

    InvalidColumnHandler(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }
}