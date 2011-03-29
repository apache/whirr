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

package org.apache.whirr.service.elasticsearch;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.whirr.service.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

public class ElasticSearchHandler extends ClusterActionHandlerSupport {

  private static final Logger LOG =
    LoggerFactory.getLogger(ElasticSearchHandler.class);

  public static final String ROLE = "elasticsearch";

  public static final int HTTP_CLIENT_PORT = 9200;

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) {
    ClusterSpec spec = event.getClusterSpec();
    Configuration config = spec.getConfiguration();

    addStatement(event, call("install_java"));
    addStatement(event, call("install_elasticsearch",
        config.getString("whirr.elasticsearch.tarball.url", "")));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec spec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Authorizing firewall port {}", HTTP_CLIENT_PORT);
    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(spec);

    FirewallSettings.authorizeIngress(computeServiceContext,
      cluster.getInstancesMatching(role(ROLE)), spec, HTTP_CLIENT_PORT);

    Configuration config = ElasticSearchConfigurationBuilder.buildConfig(spec, cluster);
    addStatement(event,
      ElasticSearchConfigurationBuilder.build("/tmp/elasticsearch.yml", config));
    addStatement(event, call("configure_elasticsearch",
      config.getStringArray("es.plugins")));
  }
}
