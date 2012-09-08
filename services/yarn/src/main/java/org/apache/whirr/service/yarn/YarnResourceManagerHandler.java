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
import static org.apache.whirr.service.yarn.YarnConfigurationBuilder.build;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;
import org.apache.whirr.service.hadoop.HadoopConfigurationConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class YarnResourceManagerHandler extends YarnHandler {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(YarnResourceManagerHandler.class);

  public static final String ROLE = "yarn-resourcemanager";
  
  public static final int RESOURCE_MANAGER_RPC_PORT = 8040;
  public static final int RESOURCE_MANAGER_WEB_UI_PORT = 8088;

  @Override
  public String getRole() {
    return ROLE;
  }
  
  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
      InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration conf = getConfiguration(clusterSpec);
    Cluster cluster = event.getCluster();
    
    Instance resourceManager = cluster.getInstanceMatching(role(ROLE));
    event.getFirewallManager().addRules(
        Rule.create()
          .destination(resourceManager)
          .ports(RESOURCE_MANAGER_RPC_PORT, RESOURCE_MANAGER_WEB_UI_PORT),
        Rule.create()
          .source(resourceManager.getPublicIp())
          .destination(resourceManager)
          .ports(RESOURCE_MANAGER_RPC_PORT)
    );

    handleFirewallRules(event);
    
    try {
      event.getStatementBuilder().addStatements(
        build("/tmp/yarn-site.xml", clusterSpec, cluster, ROLE)
      );
    } catch (ConfigurationException e) {
      throw new IOException(e);
    }
    
    addStatement(event, call(getConfigureFunction(conf)));
    addStatement(event, call(getStartFunction(conf), "resourcemanager"));
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    Instance resourceManager = cluster.getInstanceMatching(role(ROLE));
    LOG.info("Resource manager web UI available at http://{}:{}",
      resourceManager.getPublicHostName(), RESOURCE_MANAGER_WEB_UI_PORT);
    
    Properties mrConfig = createClientSideMapReduceProperties(clusterSpec);
    createClientSideMapReduceSiteFile(clusterSpec, mrConfig);

    Properties yarnConfig = createClientSideYarnProperties(clusterSpec, resourceManager);
    createClientSideYarnSiteFile(clusterSpec, yarnConfig);

    Properties combined = new Properties();
    combined.putAll(cluster.getConfiguration());
    combined.putAll(mrConfig);
    combined.putAll(yarnConfig);
    event.setCluster(new Cluster(cluster.getInstances(), combined));
  }

  private Properties createClientSideMapReduceProperties(ClusterSpec clusterSpec) throws IOException {
    Properties config = new Properties();
    config.setProperty("mapreduce.framework.name", "yarn");
    config.setProperty("mapreduce.jobhistory.address", "");
    return config;
  }
  
  private Properties createClientSideYarnProperties(ClusterSpec clusterSpec, Instance resourceManager) throws IOException {
    Properties config = new Properties();
    String prefix = "hadoop-yarn";
    Configuration sub = clusterSpec.getConfigurationForKeysWithPrefix(prefix);
    sub = sub.subset(prefix); // remove prefix
    for (@SuppressWarnings("unchecked")
        Iterator<String> it = sub.getKeys(); it.hasNext(); ) {
      String key = it.next();
      // rebuild the original value by joining all of them with the default separator
      String value = StringUtils.join(sub.getStringArray(key),
          AbstractConfiguration.getDefaultListDelimiter());
      config.setProperty(key, value);
      System.out.println("createClientSideYarnProperties " + key + ":" + value);
    }
    config.setProperty("yarn.resourcemanager.address",
        String.format("%s:8040", resourceManager.getPublicHostName()));
    config.setProperty("yarn.resourcemanager.scheduler.address",
        String.format("%s:8030", resourceManager.getPublicHostName()));
    config.setProperty("yarn.resourcemanager.resource-tracker.address",
        String.format("%s:8025", resourceManager.getPublicHostName()));
    config.setProperty("yarn.app.mapreduce.am.staging-dir", "/user");
    return config;
  }
  
  private void createClientSideMapReduceSiteFile(ClusterSpec clusterSpec, Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopSiteFile = new File(configDir, "mapred-site.xml");
    HadoopConfigurationConverter.createClientSideHadoopSiteFile(hadoopSiteFile, config);
  }

  private void createClientSideYarnSiteFile(ClusterSpec clusterSpec, Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopSiteFile = new File(configDir, "yarn-site.xml");
    HadoopConfigurationConverter.createClientSideHadoopSiteFile(hadoopSiteFile, config);
  }

  
  private File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }
}
