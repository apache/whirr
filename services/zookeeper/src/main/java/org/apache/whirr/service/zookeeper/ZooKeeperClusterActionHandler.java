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

import static org.apache.whirr.RolePredicates.role;
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
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.FirewallManager.Rule;
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

  protected Configuration getConfiguration(ClusterSpec spec)
    throws IOException {
    return getConfiguration(spec, "whirr-zookeeper-default.properties");
  }

  protected String getInstallFunction(Configuration config) {
    return getInstallFunction(config, getRole(), "install_" + getRole());
  }

  protected String getConfigureFunction(Configuration config) {
    return getConfigureFunction(config, getRole(), "configure_" + getRole());
  }

  protected String getStartFunction(Configuration config) {
    return getStartFunction(config, getRole(), "start_" + getRole());
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration config = getConfiguration(clusterSpec);

    addStatement(event, call("install_java"));
    addStatement(event, call("install_tarball"));
    addStatement(event, call("install_service"));

    String tarurl = config.getString("whirr.zookeeper.tarball.url");
    addStatement(event, call(getInstallFunction(config),
      "-u", prepareRemoteFileUrl(event, tarurl))
    );
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    event.getFirewallManager().addRule(
        Rule.create().destination(role(ZOOKEEPER_ROLE)).port(CLIENT_PORT)
    );
    
    // Pass list of all servers in ensemble to configure script.
    // Position is significant: i-th server has id i.

    Set<Instance> ensemble = cluster.getInstancesMatching(role(ZOOKEEPER_ROLE));
    String servers = Joiner.on(' ').join(getPrivateIps(ensemble));

    Configuration config = getConfiguration(clusterSpec);
    String configureFunction = getConfigureFunction(config);

    addStatement(event, call(configureFunction, servers));

    if (configureFunction.equals("configure_zookeeper")) { //default zookeeper.configure-function
      addStatement(event, call(getStartFunction(config)));
    }
    else {
      //don't call start_zookeeper, because the CDH config starts the CDH version of zookeeper on its own
    }
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    String hosts = Joiner.on(',').join(getHosts(cluster.getInstancesMatching(
      role(ZOOKEEPER_ROLE))));
    LOG.info("Hosts: {}", hosts);
  }

  private List<String> getPrivateIps(Set<Instance> instances) {
    return Lists.transform(Lists.newArrayList(instances),
        new Function<Instance, String>() {
      @Override
      public String apply(Instance instance) {
        return instance.getPrivateIp();
      }
    });
  }
  
  static List<String> getHosts(Set<Instance> instances) {
    return Lists.transform(Lists.newArrayList(instances),
        new Function<Instance, String>() {
      @Override
      public String apply(Instance instance) {
        try {
          String publicIp = instance.getPublicHostName();
          return String.format("%s:%d", publicIp, CLIENT_PORT);
        } catch (IOException e) {
          throw new IllegalArgumentException(e);
        }
      }
    });
  }

}
