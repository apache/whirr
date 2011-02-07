package org.apache.whirr.service.zookeeper;
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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.RolePredicates;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperClusterActionHandler extends ClusterActionHandlerSupport {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(ZooKeeperClusterActionHandler.class);
    
  public static final String ZOOKEEPER_ROLE = "zookeeper";
  private static final int CLIENT_PORT = 2181;

  @Override
  public String getRole() {
    return ZOOKEEPER_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    addRunUrl(event, "sun/java/install");
    addRunUrl(event, "apache/zookeeper/install");
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    LOG.info("Authorizing firewall");
    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(clusterSpec);
    FirewallSettings.authorizeIngress(computeServiceContext,
        cluster.getInstances(), clusterSpec, CLIENT_PORT);
    
    // Pass list of all servers in ensemble to configure script.
    // Position is significant: i-th server has id i.
    String servers = Joiner.on(' ').join(getPrivateIps(cluster.getInstancesMatching(
      RolePredicates.role(ZooKeeperClusterActionHandler.ZOOKEEPER_ROLE)))); 
    addRunUrl(event, "apache/zookeeper/post-configure", "-c",
        clusterSpec.getProvider(),
        servers);
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    String hosts = Joiner.on(',').join(getHosts(cluster.getInstancesMatching(
      RolePredicates.role(ZooKeeperClusterActionHandler.ZOOKEEPER_ROLE))));
    LOG.info("Hosts: {}", hosts);
  }

  private List<String> getPrivateIps(Set<Instance> instances) {
    return Lists.transform(Lists.newArrayList(instances),
        new Function<Instance, String>() {
      @Override
      public String apply(Instance instance) {
        return instance.getPrivateAddress().getHostAddress();
      }
    });
  }
  
  static List<String> getHosts(Set<Instance> instances) {
    return Lists.transform(Lists.newArrayList(instances),
        new Function<Instance, String>() {
      @Override
      public String apply(Instance instance) {
        String publicIp = instance.getPublicAddress().getHostName();
        return String.format("%s:%d", publicIp, CLIENT_PORT);
      }
    });
  }

}
