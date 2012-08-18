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

package org.apache.whirr.service.hbase;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.hadoop.HadoopConfigurationConverter;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;
import org.jclouds.scriptbuilder.domain.Statement;

import java.io.IOException;

import static org.apache.whirr.RolePredicates.role;

public class HBaseConfigurationBuilder {
  public static Statement buildHBaseSite(String path, ClusterSpec clusterSpec, Cluster cluster)
      throws ConfigurationException, IOException {
    Configuration config = buildHBaseSiteConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(HBaseConstants.FILE_HBASE_DEFAULT_PROPERTIES));
    return HadoopConfigurationConverter.asCreateXmlConfigurationFileStatement(path, config);
  }

  static Configuration buildHBaseSiteConfiguration(ClusterSpec clusterSpec, Cluster cluster, Configuration defaults)
      throws ConfigurationException, IOException {
    Configuration config = build(clusterSpec, cluster, defaults, "hbase-site");

    Cluster.Instance master = cluster.getInstanceMatching(
      role(HBaseMasterClusterActionHandler.ROLE));
    String masterHostName = master.getPublicHostName();

    config.setProperty("hbase.rootdir", String.format("hdfs://%s:8020/hbase", masterHostName));
    config.setProperty("hbase.zookeeper.quorum", ZooKeeperCluster.getHosts(cluster));

    return config;
  }

  public static Statement buildHBaseEnv(String path, ClusterSpec clusterSpec, Cluster cluster)
      throws ConfigurationException, IOException {
    Configuration config = buildHBaseEnvConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(HBaseConstants.FILE_HBASE_DEFAULT_PROPERTIES));
    return HadoopConfigurationConverter.asCreateEnvironmentVariablesFileStatement(path, config);
  }

  static Configuration buildHBaseEnvConfiguration(ClusterSpec clusterSpec, Cluster cluster, Configuration defaults)
      throws ConfigurationException, IOException {
    Configuration config = build(clusterSpec, cluster, defaults, "hbase-env");

    return config;
  }

  private static Configuration build(ClusterSpec clusterSpec, Cluster cluster, Configuration defaults, String prefix)
      throws ConfigurationException {
    CompositeConfiguration config = new CompositeConfiguration();
    Configuration sub = clusterSpec.getConfigurationForKeysWithPrefix(prefix);
    config.addConfiguration(sub.subset(prefix)); // remove prefix
    config.addConfiguration(defaults.subset(prefix));
    return config;
  }
}
