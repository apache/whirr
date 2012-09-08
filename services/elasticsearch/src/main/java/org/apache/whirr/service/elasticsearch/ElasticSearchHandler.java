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

import static org.apache.whirr.RolePredicates.role;
import static org.apache.whirr.service.FirewallManager.Rule;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

public class ElasticSearchHandler extends ClusterActionHandlerSupport {

  public static final String ROLE = "elasticsearch";

  public static final int HTTP_CLIENT_PORT = 9200;

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec spec = event.getClusterSpec();
    Configuration config = spec.getConfiguration();

    addStatement(event, call("retry_helpers"));
    addStatement(event, call("install_tarball"));

    addStatement(event, call(getInstallFunction(config, "java", "install_openjdk")));

    String tarurl = prepareRemoteFileUrl(event,
        config.getString("whirr.elasticsearch.tarball.url", ""));
    addStatement(event, call("install_elasticsearch", tarurl));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec spec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    event.getFirewallManager().addRule(
      Rule.create()
        .destination(cluster.getInstancesMatching(role(ROLE)))
        .port(HTTP_CLIENT_PORT)
    );

    handleFirewallRules(event);

    Configuration config = ElasticSearchConfigurationBuilder.buildConfig(spec, cluster);
    addStatement(event, call("retry_helpers"));
    addStatement(event,
      ElasticSearchConfigurationBuilder.build("/tmp/elasticsearch.yml", config));
    addStatement(event, call("configure_elasticsearch",
      config.getStringArray("es.plugins")));
  }

  @Override
  protected void beforeStart(ClusterActionEvent event) throws IOException {
    addStatement(event, call("start_elasticsearch"));
  }

  @Override
  protected void beforeStop(ClusterActionEvent event) throws IOException {
    addStatement(event, call("stop_elasticsearch"));
  }

  @Override
  protected void beforeCleanup(ClusterActionEvent event) throws IOException {
    addStatement(event, call("cleanup_elasticsearch"));
  }
}
