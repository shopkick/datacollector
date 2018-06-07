/*
 * Copyright 2018 StreamSets Inc.
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
package com.streamsets.pipeline.lib.salesforce.mutualauth;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.api.credential.CredentialValue;
import com.streamsets.pipeline.lib.tls.KeyStoreType;
import com.streamsets.pipeline.lib.tls.KeyStoreTypeChooserValues;
import com.streamsets.pipeline.lib.tls.TlsConfigBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * This class is a bridge between this package and the actual SslConfigBean.
 * The actual config bean can be used for initializing etc, but it can't depend on anything from this stage.
 * So we have this one which can depend on other params from this stage, and the underlying config is used to
 * actual configure SSL using JerseyClientUtil
 */
public class MutualAuthConfigBean {
  private static final Logger LOG = LoggerFactory.getLogger(MutualAuthConfigBean.class);

  @ConfigDef(
      required = true,
      defaultValue = "false",
      type = ConfigDef.Type.BOOLEAN,
      label = "Use Mutual Authentication",
      description = "If enabled, you must configure Salesforce for Mutual Authentication",
      displayPosition = 500,
      group = "ADVANCED"
  )
  public boolean useMutualAuth = false;

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Keystore File",
      description = "The path to the keystore file.  Absolute path, or relative to the Data Collector resources "
          + "directory.",
      displayPosition = 510,
      dependsOn = "useMutualAuth",
      triggeredByValue = "true",
      group = "ADVANCED"
  )
  public String keyStoreFilePath = "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.MODEL,
      defaultValue = "JKS",
      label = "Keystore Type",
      description = "The type of certificate/key scheme to use for the key tore.",
      displayPosition = 520,
      dependsOn = "useMutualAuth",
      triggeredByValue = "true",
      group = "ADVANCED"
  )
  @ValueChooserModel(KeyStoreTypeChooserValues.class)
  public KeyStoreType keyStoreType = KeyStoreType.JKS;

  @ConfigDef(
      required = false,
      type = ConfigDef.Type.CREDENTIAL,
      label = "Keystore Password",
      description = "The password to the keystore file, if applicable.  Using a password is highly recommended for "
          + "security reasons.",
      displayPosition = 530,
      dependsOn = "useMutualAuth",
      triggeredByValue = "true",
      group = "ADVANCED"
  )
  public CredentialValue keyStorePassword = () -> "";

  @ConfigDef(
      required = true,
      type = ConfigDef.Type.STRING,
      label = "Keystore Key Algorithm",
      description = "The key manager algorithm to use with the keystore.",
      defaultValue = TlsConfigBean.DEFAULT_KEY_MANAGER_ALGORITHM,
      displayPosition = 540,
      dependsOn = "useMutualAuth",
      triggeredByValue = "true",
      group = "ADVANCED"
  )
  public String keyStoreAlgorithm = TlsConfigBean.DEFAULT_KEY_MANAGER_ALGORITHM;

  private TlsConfigBean underlyingConfig;

  /**
   * Validates the parameters for this config bean.
   * @param context Stage Context
   * @param prefix Prefix to the parameter names (e.g. parent beans)
   * @param issues List of issues to augment
   */
  public void init(Stage.Context context, String prefix, List<Stage.ConfigIssue> issues) {
    underlyingConfig = new TlsConfigBean();
    underlyingConfig.tlsEnabled = true;
    if (useMutualAuth) {
      underlyingConfig.keyStorePassword = keyStorePassword;
      underlyingConfig.keyStoreFilePath = keyStoreFilePath;
      underlyingConfig.keyStoreAlgorithm = keyStoreAlgorithm;
      underlyingConfig.keyStoreType = keyStoreType;
      underlyingConfig.init(context, "TLS", prefix, issues);
      LOG.debug("Initialized Mutual Authentication config with {} keystore file {}",
          underlyingConfig.keyStoreType,
          underlyingConfig.keyStoreFilePath
      );
    }
  }

  public TlsConfigBean getUnderlyingConfig() {
    return underlyingConfig;
  }
}
