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

package org.apache.whirr.service.voldemort;

import static org.apache.whirr.RolePredicates.role;
import static org.apache.whirr.service.voldemort.VoldemortConstants.ADMIN_PORT;
import static org.apache.whirr.service.voldemort.VoldemortConstants.CLIENT_PORT;
import static org.apache.whirr.service.voldemort.VoldemortConstants.FUNCTION_CONFIGURE;
import static org.apache.whirr.service.voldemort.VoldemortConstants.FUNCTION_INSTALL;
import static org.apache.whirr.service.voldemort.VoldemortConstants.HTTP_PORT;
import static org.apache.whirr.service.voldemort.VoldemortConstants.KEY_CONF_URL;
import static org.apache.whirr.service.voldemort.VoldemortConstants.KEY_TARBALL_URL;
import static org.apache.whirr.service.voldemort.VoldemortConstants.PARAM_CONF_URL;
import static org.apache.whirr.service.voldemort.VoldemortConstants.PARAM_PARTITIONS_PER_NODE;
import static org.apache.whirr.service.voldemort.VoldemortConstants.PARAM_TARBALL_URL;
import static org.apache.whirr.service.voldemort.VoldemortConstants.ROLE;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.FirewallManager.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VoldemortClusterActionHandler extends ClusterActionHandlerSupport {

  private static final Logger LOG = LoggerFactory.getLogger(VoldemortClusterActionHandler.class);

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    Configuration config = event.getClusterSpec().getConfiguration();
    List<String> optArgs = new ArrayList<String>();

    String tarUrl = config.getString(KEY_TARBALL_URL);

    if (tarUrl != null && !tarUrl.trim().isEmpty()) {
      optArgs.add(PARAM_TARBALL_URL);
      optArgs.add(prepareRemoteFileUrl(event, tarUrl));
    }

    String confUrl = config.getString(KEY_CONF_URL);

    if (confUrl != null && !confUrl.trim().isEmpty()) {
      optArgs.add(PARAM_CONF_URL);
      optArgs.add(prepareRemoteFileUrl(event, confUrl));
    }

    addStatement(event, call("retry_helpers"));
    addStatement(event, call("install_tarball"));
    addStatement(event, call("install_service"));

    addStatement(event, call(getInstallFunction(config, "java", "install_openjdk")));

    addStatement(event, call(FUNCTION_INSTALL, optArgs.toArray(new String[optArgs.size()])));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    Cluster cluster = event.getCluster();

    LOG.info("Authorizing firewall");
    event.getFirewallManager().addRule(
      Rule.create()
        .destination(cluster.getInstancesMatching(role(ROLE)))
        .ports(CLIENT_PORT, ADMIN_PORT, HTTP_PORT)
    );

    handleFirewallRules(event);

    String servers = Joiner.on(' ').join(getPrivateIps(cluster.getInstances()));

    Configuration config = event.getClusterSpec().getConfiguration();
    int partitionsPerNode = config.getInt(PARAM_PARTITIONS_PER_NODE, 10);

    addStatement(event, call("retry_helpers"));
    addStatement(event, call(FUNCTION_CONFIGURE,
                             PARAM_PARTITIONS_PER_NODE,
                             Integer.toString(partitionsPerNode),
                             servers));
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    String servers = Joiner.on(' ').join(getPrivateIps(cluster.getInstances()));

    LOG.info("Completed setup of Voldemort {} with servers {}",
        clusterSpec.getClusterName(), servers);
  }

  @Override
  protected void beforeStart(ClusterActionEvent event) {
    addStatement(event, call("start_voldemort"));
  }

  @Override
  protected void beforeStop(ClusterActionEvent event) {
    addStatement(event, call("stop_voldemort"));
  }

  @Override
  protected void beforeCleanup(ClusterActionEvent event) {
    addStatement(event, call("remove_service"));
    addStatement(event, call("cleanup_voldemort"));
  }

  /**
   * Given a set of instances returns a list of their private IPs.
   * 
   * @param instances Set of instances in the cluster
   * @return List of all private IPs as strings
   */
  private List<String> getPrivateIps(Set<Instance> instances) {
    return Lists.transform(Lists.newArrayList(instances), new Function<Instance, String>() {

      @Override
      public String apply(Instance instance) {
        return instance.getPrivateIp();
      }
    });
  }

}
