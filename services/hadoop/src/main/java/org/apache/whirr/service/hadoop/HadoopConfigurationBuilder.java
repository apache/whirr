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

package org.apache.whirr.service.hadoop;

import static org.apache.whirr.RolePredicates.role;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Processor;
import org.jclouds.scriptbuilder.domain.Statement;

public class HadoopConfigurationBuilder {
  
  private static final String WHIRR_HADOOP_DEFAULT_PROPERTIES =
    "whirr-hadoop-default.properties";

  private static Configuration build(ClusterSpec clusterSpec, Cluster cluster,
      Configuration defaults, String prefix)
      throws ConfigurationException {
    CompositeConfiguration config = new CompositeConfiguration();
    Configuration sub = clusterSpec.getConfigurationForKeysWithPrefix(prefix);
    config.addConfiguration(sub.subset(prefix)); // remove prefix
    config.addConfiguration(defaults.subset(prefix));
    return config;
  }

  public static Statement buildCommon(String path, ClusterSpec clusterSpec,
      Cluster cluster) throws ConfigurationException, IOException {
    Configuration config = buildCommonConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(WHIRR_HADOOP_DEFAULT_PROPERTIES));
    return HadoopConfigurationConverter.asCreateXmlConfigurationFileStatement(path, config);
  }
  
  public static Statement buildHdfs(String path, ClusterSpec clusterSpec,
      Cluster cluster) throws ConfigurationException, IOException {
    Configuration config = buildHdfsConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(WHIRR_HADOOP_DEFAULT_PROPERTIES));
    return HadoopConfigurationConverter.asCreateXmlConfigurationFileStatement(path, config);
  }
  
  public static Statement buildMapReduce(String path, ClusterSpec clusterSpec,
      Cluster cluster) throws ConfigurationException, IOException {
    Configuration config = buildMapReduceConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(WHIRR_HADOOP_DEFAULT_PROPERTIES));
    return HadoopConfigurationConverter.asCreateXmlConfigurationFileStatement(path, config);
  }
  
  public static Statement buildHadoopEnv(String path, ClusterSpec clusterSpec,
      Cluster cluster) throws ConfigurationException, IOException {
    Configuration config = buildHadoopEnvConfiguration(clusterSpec, cluster,
        new PropertiesConfiguration(WHIRR_HADOOP_DEFAULT_PROPERTIES));
    return HadoopConfigurationConverter.asCreateEnvironmentVariablesFileStatement(path, config);
  }
  
  @VisibleForTesting
  static Configuration buildCommonConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, Configuration defaults) throws ConfigurationException, IOException {
    Configuration config = build(clusterSpec, cluster, defaults,
        "hadoop-common");

    Instance namenode = cluster
        .getInstanceMatching(role(HadoopNameNodeClusterActionHandler.ROLE));
    config.setProperty("fs.default.name", String.format("hdfs://%s:8020/",
        namenode.getPublicAddress().getHostName()));
    return config;
  }
  
  @VisibleForTesting
  static Configuration buildHdfsConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, Configuration defaults) throws ConfigurationException {
    return build(clusterSpec, cluster, defaults, "hadoop-hdfs");
  }
  
  @VisibleForTesting
  static Configuration buildMapReduceConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, Configuration defaults) throws ConfigurationException, IOException {
    Configuration config = build(clusterSpec, cluster, defaults,
        "hadoop-mapreduce");
    
    Set<Instance> taskTrackers = cluster
      .getInstancesMatching(role(HadoopTaskTrackerClusterActionHandler.ROLE));
    
    if (!taskTrackers.isEmpty()) {
      
      Hardware hardware = Iterables.getFirst(taskTrackers, null)
        .getNodeMetadata().getHardware();

      /* null when using the BYON jclouds compute provider */
      if (hardware != null) {

        int coresPerNode = 0;
        for (Processor processor : hardware.getProcessors()) {
          coresPerNode += processor.getCores();
        }
        int mapTasksPerNode = (int) Math.ceil(coresPerNode * 1.0);
        int reduceTasksPerNode = (int) Math.ceil(coresPerNode * 0.75);

        setIfAbsent(config, "mapred.tasktracker.map.tasks.maximum", mapTasksPerNode + "");
        setIfAbsent(config, "mapred.tasktracker.reduce.tasks.maximum", reduceTasksPerNode + "");

        int clusterReduceSlots = taskTrackers.size() * reduceTasksPerNode;
        setIfAbsent(config, "mapred.reduce.tasks", clusterReduceSlots + "");

      }
    }

    Instance jobtracker = cluster
        .getInstanceMatching(role(HadoopJobTrackerClusterActionHandler.ROLE));
    config.setProperty("mapred.job.tracker", String.format("%s:8021",
        jobtracker.getPublicAddress().getHostName()));
    return config;
  }

  @VisibleForTesting
  static Configuration buildHadoopEnvConfiguration(ClusterSpec clusterSpec,
      Cluster cluster, Configuration defaults) throws ConfigurationException {
    return build(clusterSpec, cluster, defaults, "hadoop-env");
  }
  
  private static void setIfAbsent(Configuration config, String property, String value) {
    if (!config.containsKey(property)) {
      config.setProperty(property, value);
    }
  }

}
