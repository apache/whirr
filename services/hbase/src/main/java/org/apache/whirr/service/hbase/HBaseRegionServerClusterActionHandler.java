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

import org.apache.whirr.net.DnsUtil;
import org.apache.whirr.service.*;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;
import org.jclouds.compute.ComputeServiceContext;

import java.io.IOException;
import java.net.InetAddress;

import static org.apache.whirr.service.RolePredicates.role;

public class HBaseRegionServerClusterActionHandler extends ClusterActionHandlerSupport {

  public static final String ROLE = "hbase-regionserver";

  public static final int REGIONSERVER_PORT = 60020;
  public static final int REGIONSERVER_WEB_UI_PORT = 60030;

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    addRunUrl(event, "util/configure-hostnames", "-c", clusterSpec.getProvider());
    String hbaseInstallRunUrl = clusterSpec.getConfiguration().getString(
        "whirr.hbase-install-runurl", "apache/hbase/install");
    addRunUrl(event, "sun/java/install");
    addRunUrl(event, hbaseInstallRunUrl, "-c", clusterSpec.getProvider());
    event.setTemplateBuilderStrategy(new HBaseTemplateBuilderStrategy());
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    Instance instance = cluster.getInstanceMatching(
      role(HBaseMasterClusterActionHandler.ROLE));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(clusterSpec);
    FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
      REGIONSERVER_WEB_UI_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
        masterPublicAddress.getHostAddress(), REGIONSERVER_PORT);

    String hbaseConfigureRunUrl = clusterSpec.getConfiguration().getString(
      "whirr.hbase-configure-runurl", "apache/hbase/post-configure");
    String quorum = ZooKeeperCluster.getHosts(cluster);
    addRunUrl(event, hbaseConfigureRunUrl, ROLE,
      "-m", DnsUtil.resolveAddress(masterPublicAddress.getHostAddress()),
      "-q", quorum,
      "-c", clusterSpec.getProvider());
  }

}
