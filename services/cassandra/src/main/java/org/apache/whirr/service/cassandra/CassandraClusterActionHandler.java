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

package org.apache.whirr.service.cassandra;

import static org.apache.whirr.RolePredicates.role;
import static org.apache.whirr.service.FirewallManager.Rule;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

public class CassandraClusterActionHandler extends ClusterActionHandlerSupport {

  public static final String CASSANDRA_ROLE = "cassandra";
  public static final int CLIENT_PORT = 9160;
  public static final int JMX_PORT = 8080;

  public static final String BIN_TARBALL = "whirr.cassandra.tarball.url";
  public static final String MAJOR_VERSION = "whirr.cassandra.version.major";

  @Override
  public String getRole() {
    return CASSANDRA_ROLE;
  }
  
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    addStatement(event, call("install_java"));
    addStatement(event, call("install_tarball"));

    addStatement(event, call("install_service"));
    addStatement(event, call("remove_service"));

    Configuration config = event.getClusterSpec().getConfiguration();

    String tarball = prepareRemoteFileUrl(event, config.getString(BIN_TARBALL, null));
    String major = config.getString(MAJOR_VERSION, null);

    if (tarball != null && major != null) {
      addStatement(event, call("install_cassandra", major, tarball));
    } else {
      addStatement(event, call("install_cassandra"));
    }
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    Cluster cluster = event.getCluster();

    event.getFirewallManager().addRule(
      Rule.create()
        .destination(cluster.getInstancesMatching(role(CASSANDRA_ROLE)))
        .ports(CLIENT_PORT, JMX_PORT)
    );

    List<Instance> seeds = getSeeds(cluster.getInstances());
    String servers = Joiner.on(' ').join(getPrivateIps(seeds));
    addStatement(event, call("configure_cassandra", servers));
    addStatement(event, call("start_cassandra"));
  }

  private List<String> getPrivateIps(List<Instance> instances) {
    return Lists.transform(Lists.newArrayList(instances),
        new Function<Instance, String>() {
      @Override
      public String apply(Instance instance) {
        return instance.getPrivateIp();
      }
    });
  }
  
  /**
   * Pick a selection of the nodes that are to become seeds. TODO improve
   * selection method. Right now it picks 20% of the nodes as seeds, or a
   * minimum of one node if it is a small cluster.
   * 
   * @param instances
   *          all nodes in cluster
   * @return list of seeds
   */
  protected List<Instance> getSeeds(Set<Instance> instances) {
    List<Instance> nodes = Lists.newArrayList(instances);
    int seeds = (int) Math.ceil(Math.max(1, instances.size() * 0.2));
    List<Instance> rv = Lists.newArrayList();
    for (int i = 0; i < seeds; i++) {
      rv.add(nodes.get(i));
    }
    return rv;
  }

}
