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
import static org.apache.whirr.service.hbase.HBaseConfigurationBuilder.buildHBaseEnv;
import static org.apache.whirr.service.hbase.HBaseConfigurationBuilder.buildHBaseSite;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;

public class HBaseRegionServerClusterActionHandler extends HBaseClusterActionHandler {

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

    addStatement(event, call("configure_hostnames"));

    addStatement(event, call("install_java"));
    addStatement(event, call("install_tarball"));

    String tarurl = prepareRemoteFileUrl(event,
      getConfiguration(clusterSpec).getString(HBaseConstants.KEY_TARBALL_URL));

    addStatement(event, call(
      getInstallFunction(getConfiguration(clusterSpec)),
      HBaseConstants.PARAM_TARBALL_URL, tarurl)
    );
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    Configuration conf = getConfiguration(clusterSpec);

    Instance instance = cluster.getInstanceMatching(
      role(HBaseMasterClusterActionHandler.ROLE));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    event.getFirewallManager().addRules(
      Rule.create()
        .destination(instance)
        .ports(REGIONSERVER_WEB_UI_PORT, REGIONSERVER_PORT)
    );

    try {
      event.getStatementBuilder().addStatements(
          buildHBaseSite("/tmp/hbase-site.xml", clusterSpec, cluster),
          buildHBaseEnv("/tmp/hbase-env.sh", clusterSpec, cluster)
      );
    } catch (ConfigurationException e) {
      throw new IOException(e);
    }

    String master = masterPublicAddress.getHostName();
    String quorum = ZooKeeperCluster.getHosts(cluster);

    String tarurl = prepareRemoteFileUrl(event,
      conf.getString(HBaseConstants.KEY_TARBALL_URL));

    addStatement(event, call(
      getConfigureFunction(conf),
      ROLE,
      HBaseConstants.PARAM_MASTER, master,
      HBaseConstants.PARAM_QUORUM, quorum,
      HBaseConstants.PARAM_TARBALL_URL, tarurl)
    );
  }

}
