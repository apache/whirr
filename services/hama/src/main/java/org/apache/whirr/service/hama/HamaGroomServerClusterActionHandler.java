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
package org.apache.whirr.service.hama;

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;

public class HamaGroomServerClusterActionHandler extends
    HamaClusterActionHandler {
  public static final String ROLE = "hama-groomserver";
  public static final int GROOMSERVER_PORT = 50000;

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
      InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    Instance instance = cluster
        .getInstanceMatching(role(HamaMasterClusterActionHandler.ROLE));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    event.getFirewallManager().addRules(
        Rule.create().destination(instance).ports(61000, GROOMSERVER_PORT));

    String hamaConfigureFunction = getConfiguration(clusterSpec).getString(
        HamaConstants.KEY_CONFIGURE_FUNCTION,
        HamaConstants.FUNCTION_POST_CONFIGURE);

    String master = masterPublicAddress.getHostName();
    String quorum = ZooKeeperCluster.getHosts(cluster);

    String tarurl = prepareRemoteFileUrl(event, getConfiguration(clusterSpec)
        .getString(HamaConstants.KEY_TARBALL_URL));

    addStatement(event, call(hamaConfigureFunction, ROLE,
        HamaConstants.PARAM_MASTER, master, HamaConstants.PARAM_QUORUM, quorum,
        HamaConstants.PARAM_TARBALL_URL, tarurl));

    String hamaStartFunction = getConfiguration(clusterSpec).getString(
        HamaConstants.KEY_START_FUNCTION, HamaConstants.FUNCTION_START);

    addStatement(event, call(hamaStartFunction, ROLE,
        HamaConstants.PARAM_TARBALL_URL, tarurl));
  }
}
