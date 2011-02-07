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

package org.apache.whirr.cluster.actions;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.RolePredicates;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.whirr.service.ClusterAction} for running a configuration script on instances
 * in the cluster after it has been bootstrapped.
 */
public class ConfigureClusterAction extends ScriptBasedClusterAction {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(ConfigureClusterAction.class);

  @Override
  protected String getAction() {
    return ClusterActionHandler.CONFIGURE_ACTION;
  }
  
  @Override
  protected void doAction(Map<InstanceTemplate, ClusterActionEvent> eventMap)
      throws IOException {
    
    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap.entrySet()) {
      ClusterSpec clusterSpec = entry.getValue().getClusterSpec();
      Cluster cluster = entry.getValue().getCluster();
      StatementBuilder statementBuilder = entry.getValue().getStatementBuilder();
      ComputeServiceContext computeServiceContext =
        ComputeServiceContextBuilder.build(clusterSpec);
      ComputeService computeService = computeServiceContext.getComputeService();
      Credentials credentials = new Credentials(
          Iterables.get(cluster.getInstances(), 0).getLoginCredentials().identity,
          clusterSpec.getPrivateKey());
      try {
        LOG.info("Running configuration script");
        if (LOG.isDebugEnabled())
          LOG.debug("Running script:\n{}", statementBuilder.render(OsFamily.UNIX));
        computeService.runScriptOnNodesMatching(
            toNodeMetadataPredicate(clusterSpec, cluster, entry.getKey().getRoles()),
            statementBuilder,
            RunScriptOptions.Builder.overrideCredentialsWith(credentials));
        LOG.info("Configuration script run completed");
      } catch (RunScriptOnNodesException e) {
        // TODO: retry
        throw new IOException(e);
      }
    }
  }
  
  private Predicate<NodeMetadata> toNodeMetadataPredicate(final ClusterSpec clusterSpec, final Cluster cluster, final Set<String> roles) {
    final Map<String, Instance> nodeIdToInstanceMap = Maps.newHashMap();
    for (Instance instance : cluster.getInstances()) {
      nodeIdToInstanceMap.put(instance.getId(), instance);
    }
    return new Predicate<NodeMetadata>() {
      @Override
      public boolean apply(NodeMetadata nodeMetadata) {
        // Check it's the correct cluster
        if (!nodeMetadata.getGroup().equals(clusterSpec.getClusterName())) {
          return false;
        }
        Instance instance = nodeIdToInstanceMap.get(nodeMetadata.getId());
        if (instance == null) {
          LOG.debug("No instance for {} found in map", nodeMetadata);
          return false;
        }
        return RolePredicates.onlyRolesIn(roles).apply(instance);
      }
      @Override
      public String toString() {
         return "roles(" + roles + ")";
      }
   };
  }
}
