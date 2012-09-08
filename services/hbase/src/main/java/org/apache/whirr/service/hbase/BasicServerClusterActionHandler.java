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

import static org.apache.whirr.RolePredicates.role;
import static org.apache.whirr.service.FirewallManager.Rule;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import org.apache.commons.configuration.Configuration;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;

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
    Configuration conf = getConfiguration(clusterSpec);

    addStatement(event, call("retry_helpers"));
    addStatement(event, call("configure_hostnames"));
    addStatement(event, call("install_tarball"));

    addStatement(event, call(getInstallFunction(conf, "java", "install_openjdk")));

    String tarurl = prepareRemoteFileUrl(event,
      conf.getString(HBaseConstants.KEY_TARBALL_URL));

    addStatement(event, call(
      getInstallFunction(conf),
      HBaseConstants.PARAM_TARBALL_URL, tarurl)
    );
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

    Cluster.Instance instance = cluster.getInstanceMatching(role(role));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    event.getFirewallManager().addRule(
      Rule.create().destination(instance).port(port)
    );

    handleFirewallRules(event);
    
    String master = masterPublicAddress.getHostName();
    String quorum = ZooKeeperCluster.getHosts(cluster);

    String tarurl = prepareRemoteFileUrl(event,
      getConfiguration(clusterSpec).getString(HBaseConstants.KEY_TARBALL_URL));

    addStatement(event, call("retry_helpers"));
    addStatement(event, call(
      getConfigureFunction(getConfiguration(clusterSpec)),
      role,
      HBaseConstants.PARAM_MASTER, master,
      HBaseConstants.PARAM_QUORUM, quorum,
      HBaseConstants.PARAM_PORT, Integer.toString(port),
      HBaseConstants.PARAM_TARBALL_URL, tarurl)
    );
  }

}
