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

package org.apache.whirr.actions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.FirewallManager.Rule;
import org.jclouds.compute.ComputeServiceContext;

import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;
import com.google.common.primitives.Ints;


/**
 * A {@link org.apache.whirr.ClusterAction} for running a configuration script on instances
 * in the cluster after it has been bootstrapped.
 */
public class ConfigureServicesAction extends ScriptBasedClusterAction {

  public ConfigureServicesAction(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      LoadingCache<String, ClusterActionHandler> handlerMap
  ) {
    super(getCompute, handlerMap);
  }

  public ConfigureServicesAction(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      LoadingCache<String, ClusterActionHandler> handlerMap,
      Set<String> targetRoles,
      Set<String> targetInstanceIds
  ) {
    super(getCompute, handlerMap, targetRoles, targetInstanceIds);
  }

  @Override
  protected String getAction() {
    return ClusterActionHandler.CONFIGURE_ACTION;
  }

  /**
   * Apply the firewall rules specified via configuration.
   */
  protected void eventSpecificActions(InstanceTemplate instanceTemplate, ClusterActionEvent event) 
      throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    
    Map<String, List<String>> firewallRules = clusterSpec.getFirewallRules();
    for (String role: firewallRules.keySet()) {
      if (!roleIsInTarget(role)) {
        continue;   // skip execution for this role
      }
      Rule rule = Rule.create();
      
      if (role == null) {
        rule.destination(event.getCluster().getInstances());
      } else {
        rule.destination(RolePredicates.role(role));
      }
      
      List<String> ports = firewallRules.get(role);
      rule.ports(Ints.toArray(Collections2.transform(ports, new Function<String,Integer>() {
        @Override
        public Integer apply(String input) {
          return Integer.valueOf(input);
        }
      })));

      event.getFirewallManager().addRule(rule);
    }
  }

}
