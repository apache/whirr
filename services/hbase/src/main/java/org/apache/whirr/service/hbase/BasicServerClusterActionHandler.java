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

import static org.apache.whirr.service.RolePredicates.role;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.whirr.net.DnsUtil;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;
import org.jclouds.compute.ComputeServiceContext;

/**
 * Provides a base class for servers like REST or Avro.
 */
public class BasicServerClusterActionHandler extends HBaseClusterActionHandler {

  private final String role;
  private final int defaultPort;
  private final String configKeyPort;

  public BasicServerClusterActionHandler(String role, int port, String configKeyPort) {
    this.role = role;
    this.defaultPort = port;
    this.configKeyPort = configKeyPort;
  }

  @Override
  public String getRole() {
    return role;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    addRunUrl(event, "util/configure-hostnames",
      HBaseConstants.PARAM_PROVIDER, clusterSpec.getProvider());
    addRunUrl(event, "sun/java/install");
    String hbaseInstallRunUrl = getConfiguration(clusterSpec).getString(
      HBaseConstants.KEY_INSTALL_RUNURL, HBaseConstants.SCRIPT_INSTALL);
    String tarurl = getConfiguration(clusterSpec).getString(
      HBaseConstants.KEY_TARBALL_URL);
    addRunUrl(event, hbaseInstallRunUrl,
      HBaseConstants.PARAM_PROVIDER, clusterSpec.getProvider(),
      HBaseConstants.PARAM_TARBALL_URL, tarurl);
    event.setTemplateBuilderStrategy(new HBaseTemplateBuilderStrategy());
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    int port = defaultPort;
    if (configKeyPort != null) {
      port = getConfiguration(clusterSpec).getInt(configKeyPort, defaultPort);
    }

    Cluster.Instance instance = cluster.getInstanceMatching(
      role(HBaseMasterClusterActionHandler.ROLE));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(clusterSpec);
    FirewallSettings.authorizeIngress(computeServiceContext, instance,
      clusterSpec, port);

    String hbaseConfigureRunUrl = getConfiguration(clusterSpec).getString(
      HBaseConstants.KEY_CONFIGURE_RUNURL,
      HBaseConstants.SCRIPT_POST_CONFIGURE);
    String master = DnsUtil.resolveAddress(masterPublicAddress.getHostAddress());
    String quorum = ZooKeeperCluster.getHosts(cluster);
    String tarurl = getConfiguration(clusterSpec).getString(
      HBaseConstants.KEY_TARBALL_URL);   
    addRunUrl(event, hbaseConfigureRunUrl, role,
      HBaseConstants.PARAM_MASTER, master,
      HBaseConstants.PARAM_QUORUM, quorum,
      HBaseConstants.PARAM_PORT, Integer.toString(port),
      HBaseConstants.PARAM_PROVIDER, clusterSpec.getProvider(),
      HBaseConstants.PARAM_TARBALL_URL, tarurl);
  }

}
