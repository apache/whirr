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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.compute.predicates.NodePredicates;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.whirr.ClusterAction} for running a configuration script on instances
 * in the cluster after it has been bootstrapped.
 */
public class ConfigureClusterAction extends ScriptBasedClusterAction {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(ConfigureClusterAction.class);

  public ConfigureClusterAction(Function<ClusterSpec, ComputeServiceContext> getCompute,
      Map<String, ClusterActionHandler> handlerMap) {
    super(getCompute, handlerMap);
  }
  
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

      ComputeServiceContext computeServiceContext = getCompute().apply(clusterSpec);
      ComputeService computeService = computeServiceContext.getComputeService();

      Credentials credentials = new Credentials(
          clusterSpec.getClusterUser(),
          clusterSpec.getPrivateKey());

      try {
        Map<String, ? extends NodeMetadata> nodesInCluster = getNodesForInstanceIdsInCluster(cluster, computeService);
        
        if (LOG.isDebugEnabled()) {
          LOG.debug("Nodes in cluster: {}", nodesInCluster.values());
        }
        
        Map<String, ? extends NodeMetadata> nodesToApply = Maps.uniqueIndex(
            Iterables.filter(nodesInCluster.values(),
                toNodeMetadataPredicate(clusterSpec, cluster, entry.getKey().getRoles())),
            getNodeId
        );

        LOG.info("Running configuration script on nodes: {}", nodesToApply.keySet());
        if (LOG.isDebugEnabled())
          LOG.debug("script:\n{}", statementBuilder.render(OsFamily.UNIX));
        
        computeService.runScriptOnNodesMatching(
            withIds(nodesToApply.keySet()),
            statementBuilder,
            RunScriptOptions.Builder.overrideCredentialsWith(credentials)
        );
        
        LOG.info("Configuration script run completed");
      } catch (RunScriptOnNodesException e) {
        // TODO: retry
        throw new IOException(e);
      }
    }
  }

  private Map<String, ? extends NodeMetadata> getNodesForInstanceIdsInCluster(Cluster cluster,
        ComputeService computeService) {
    Iterable<String> ids = Iterables.transform(cluster.getInstances(), new Function<Instance, String>() {

      @Override
      public String apply(Instance arg0) {
        return arg0.getId();
      }

    });
    
    Set<? extends NodeMetadata> nodes = computeService.listNodesDetailsMatching(
        NodePredicates.withIds(Iterables.toArray(ids, String.class)));
    
    return Maps.uniqueIndex(nodes, getNodeId);
  }

  public static Predicate<NodeMetadata> withIds(Iterable<String> ids) {
    checkNotNull(ids, "ids must be defined");
    final Set<String> search = ImmutableSet.copyOf(ids);
    return new Predicate<NodeMetadata>() {
      @Override
      public boolean apply(NodeMetadata nodeMetadata) {
        return search.contains(nodeMetadata.getId());
      }
    };
  }
  
  private static Function<NodeMetadata, String> getNodeId = new Function<NodeMetadata, String>() {

    @Override
    public String apply(NodeMetadata arg0) {
      return arg0.getId();
    }
     
  };
  
  private Predicate<NodeMetadata> toNodeMetadataPredicate(final ClusterSpec clusterSpec, final Cluster cluster, final Set<String> roles) {
    final Map<String, Instance> nodeIdToInstanceMap = Maps.newHashMap();
    for (Instance instance : cluster.getInstances()) {
      nodeIdToInstanceMap.put(instance.getId(), instance);
    }
    return new Predicate<NodeMetadata>() {
      @Override
      public boolean apply(NodeMetadata nodeMetadata) {
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
