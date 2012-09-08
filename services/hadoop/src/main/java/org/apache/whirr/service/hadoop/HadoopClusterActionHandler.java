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

import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildCommon;
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildHadoopEnv;
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildHdfs;
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildMapReduce;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.template.TemplateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HadoopClusterActionHandler extends ClusterActionHandlerSupport {

  private static final Logger LOG =
      LoggerFactory.getLogger(HadoopClusterActionHandler.class);

  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with a hadoop defaults
   * properties.
   */
  protected Configuration getConfiguration(
      ClusterSpec clusterSpec) throws IOException {
    return getConfiguration(clusterSpec, "whirr-hadoop-default.properties");
  }

  protected String getInstallFunction(Configuration config) {
    return getInstallFunction(config, "hadoop", "install_hadoop");
  }

  protected String getConfigureFunction(Configuration config) {
    return getConfigureFunction(config, "hadoop", "configure_hadoop");
  }
  
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration conf = getConfiguration(clusterSpec);

    addStatement(event, call("retry_helpers"));
    addStatement(event, call("configure_hostnames"));
    addStatement(event, call("install_tarball"));

    addStatement(event, call(getInstallFunction(conf, "java", "install_openjdk")));

    String tarball = prepareRemoteFileUrl(event,
        conf.getString("whirr.hadoop.tarball.url"));

    addStatement(event, call(getInstallFunction(conf),
        "-u", tarball));
  }
  
  protected Map<String, String> getDeviceMappings(ClusterActionEvent event) {
      Set<Instance> instances = event.getCluster().getInstancesMatching(RolePredicates.role(getRole()));
      Instance prototype = Iterables.getFirst(instances, null);
      if (prototype == null) {
          throw new IllegalStateException("No instances found in role " + getRole());
      }
      VolumeManager volumeManager = new VolumeManager();
      return volumeManager.getDeviceMappings(event.getClusterSpec(), prototype);
  }
    
  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    doBeforeConfigure(event);

    handleFirewallRules(event);
    
    createHadoopConfigFiles(event, clusterSpec, cluster);

    addStatement(event, call("retry_helpers"));
    addStatement(event, call(
      getConfigureFunction(getConfiguration(clusterSpec)),
      Joiner.on(",").join(event.getInstanceTemplate().getRoles()),
      "-c", clusterSpec.getProvider())
    );
  }

  protected void doBeforeConfigure(ClusterActionEvent event) throws IOException {};

  private void createHadoopConfigFiles(ClusterActionEvent event,
      ClusterSpec clusterSpec, Cluster cluster) throws IOException {
    Map<String, String> deviceMappings = getDeviceMappings(event);

    //Velocity is assuming flat classloaders or TCCL to load templates.
    //This doesn't work in OSGi unless we set the TCCL to the bundle classloader before invocation
    ClassLoader oldTccl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      event.getStatementBuilder().addStatements(
        buildCommon("/tmp/core-site.xml", clusterSpec, cluster),
        buildHdfs("/tmp/hdfs-site.xml", clusterSpec, cluster, deviceMappings.keySet()),
        buildMapReduce("/tmp/mapred-site.xml", clusterSpec, cluster, deviceMappings.keySet()),
        buildHadoopEnv("/tmp/hadoop-env.sh", clusterSpec, cluster),
        TemplateUtils.createFileFromTemplate("/tmp/hadoop-metrics.properties", event.getTemplateEngine(), getMetricsTemplate(event, clusterSpec, cluster), clusterSpec, cluster)
      );
      
    } catch (ConfigurationException e) {
      throw new IOException(e);
    }  finally {
      Thread.currentThread().setContextClassLoader(oldTccl);
    }
    String devMappings = VolumeManager.asString(deviceMappings);
    addStatement(event, call("prepare_all_disks", "'" + devMappings + "'"));
  }

  private String getMetricsTemplate(ClusterActionEvent event, ClusterSpec clusterSpec, Cluster cluster) {
    Configuration conf = clusterSpec.getConfiguration();
    if (conf.containsKey("hadoop-metrics.template")) {
      return conf.getString("hadoop-metrics.template");
    }
    
    Set<Instance> gmetadInstances = cluster.getInstancesMatching(RolePredicates.role("ganglia-metad"));
    if (!gmetadInstances.isEmpty()) {
      return "hadoop-metrics-ganglia.properties.vm";
    }
    
    return "hadoop-metrics-null.properties.vm";
  }

}
