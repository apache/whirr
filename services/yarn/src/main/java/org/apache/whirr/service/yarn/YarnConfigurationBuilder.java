/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.yarn;

import static org.apache.whirr.RolePredicates.role;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.hadoop.HadoopConfigurationConverter;
import org.jclouds.scriptbuilder.domain.Statement;

public class YarnConfigurationBuilder {
  
  private static final String WHIRR_YARN_DEFAULT_PROPERTIES =
    "whirr-yarn-default.properties";

  private static Configuration build(ClusterSpec clusterSpec, Cluster cluster,
      Configuration defaults, String prefix)
      throws ConfigurationException {
    CompositeConfiguration config = new CompositeConfiguration();
    Configuration sub = clusterSpec.getConfigurationForKeysWithPrefix(prefix);
    config.addConfiguration(sub.subset(prefix)); // remove prefix
    config.addConfiguration(defaults.subset(prefix));
    return config;
  }

  public static Statement build(String path, ClusterSpec clusterSpec,
      Cluster cluster, String role) throws ConfigurationException, IOException {
    Configuration config = buildConfiguration(clusterSpec, cluster, role,
        new PropertiesConfiguration(WHIRR_YARN_DEFAULT_PROPERTIES));
    return HadoopConfigurationConverter.asCreateXmlConfigurationFileStatement(path, config);
  }
  
  @VisibleForTesting
  static Configuration buildConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, String role, Configuration defaults) throws ConfigurationException,
      IOException {
    Configuration config = build(clusterSpec, cluster, defaults,
        "hadoop-yarn");

    Instance resourceManager = cluster
        .getInstanceMatching(role(YarnResourceManagerHandler.ROLE));
    String resourceManagerPrivateAddress =
      resourceManager.getPrivateAddress().getHostName();
    config.setProperty("yarn.resourcemanager.address",
        String.format("%s:8040", resourceManagerPrivateAddress));
    config.setProperty("yarn.resourcemanager.scheduler.address",
        String.format("%s:8030", resourceManagerPrivateAddress));
    config.setProperty("yarn.resourcemanager.resource-tracker.address",
        String.format("%s:8025", resourceManagerPrivateAddress));
    return config;
  }
}
