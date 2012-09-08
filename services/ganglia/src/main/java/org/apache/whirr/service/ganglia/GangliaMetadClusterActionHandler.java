package org.apache.whirr.service.ganglia;
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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class GangliaMetadClusterActionHandler extends ClusterActionHandlerSupport {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(GangliaMetadClusterActionHandler.class);
    
  public static final String GANGLIA_METAD_ROLE = "ganglia-metad";
  public static final int GANGLIA_META_PORT = 8651;
  public static final int HTTP_PORT = 80;

  @Override
  public String getRole() {
    return GANGLIA_METAD_ROLE;
  }

  protected Configuration getConfiguration(ClusterSpec spec)
    throws IOException {
    return getConfiguration(spec, "whirr-ganglia-default.properties");
  }

  protected String getInstallFunction(Configuration config) {
    return getInstallFunction(config, getRole(), GangliaCluster.INSTALL_FUNCTION);
  }

  protected String getConfigureFunction(Configuration config) {
    return getConfigureFunction(config, getRole(), GangliaCluster.CONFIGURE_FUNCTION);
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration config = getConfiguration(clusterSpec);

    addStatement(event, call("retry_helpers"));
    addStatement(event, call(getInstallFunction(config),
      "-r", GANGLIA_METAD_ROLE)
    );
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    // FIXME: the ganglia port is only opened so the ganglia xml dump can be checked in GangliaServiceTest
    event.getFirewallManager().addRule(
        Rule.create().destination(role(GANGLIA_METAD_ROLE)).ports(HTTP_PORT, GANGLIA_META_PORT)
    );

    handleFirewallRules(event);

    Configuration config = getConfiguration(clusterSpec);
    String configureFunction = getConfigureFunction(config);

    // Call the configure function.
    String metadHost = cluster.getInstanceMatching(role(GANGLIA_METAD_ROLE)).getPrivateIp();
    addStatement(event, call("retry_helpers"));
    addStatement(event, call(configureFunction, "-m", metadHost));
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    String hosts = Joiner.on(',').join(getHosts(cluster.getInstancesMatching(
      role(GANGLIA_METAD_ROLE))));
    LOG.info("Meta host: {}. You should be able to connect on http://{}/ganglia", hosts, hosts);
  }

  static List<String> getHosts(Set<Instance> instances) {
      return Lists.transform(Lists.newArrayList(instances),
          new Function<Instance, String>() {
        @Override
        public String apply(Instance instance) {
          try {
            return instance.getPublicHostName();
          } catch (IOException e) {
            throw new IllegalArgumentException(e);
          }
        }
      });
    }

}
