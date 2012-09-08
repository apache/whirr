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

package org.apache.whirr.service.yarn;

import static org.apache.whirr.RolePredicates.role;
import static org.apache.whirr.service.yarn.YarnConfigurationBuilder.build;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;

public class YarnNodeManagerHandler extends YarnHandler {

  public static final String ROLE = "yarn-nodemanager";
  
  public static final int NODE_MANAGER_WEB_UI_PORT = 9999;

  @Override
  public String getRole() {
    return ROLE;
  }
  
  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
      InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration conf = getConfiguration(clusterSpec);
    Cluster cluster = event.getCluster();
    
    Set<Instance> nodeManagers = cluster.getInstancesMatching(role(ROLE));
    if (!nodeManagers.isEmpty()) {
        for(Instance nodeManager : nodeManagers) {
            event.getFirewallManager().addRules(
                    Rule.create()
                      .destination(nodeManager)
                      .ports(NODE_MANAGER_WEB_UI_PORT)
                );            
        }
    }

    handleFirewallRules(event);
    
    try {
      event.getStatementBuilder().addStatements(
        build("/tmp/yarn-site.xml", clusterSpec, cluster, ROLE)
      );
    } catch (ConfigurationException e) {
      throw new IOException(e);
    }
    
    addStatement(event, call(getConfigureFunction(conf)));
    addStatement(event, call(getStartFunction(conf), "nodemanager"));
  }
  
}
