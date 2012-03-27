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

package org.apache.whirr;

import static org.apache.whirr.service.ClusterActionHandler.BOOTSTRAP_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.CONFIGURE_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.DESTROY_ACTION;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.actions.ByonClusterAction;
import org.apache.whirr.service.ClusterActionHandler;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.scriptbuilder.domain.Statement;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Collections2;

/**
 * Equivalent of {@link ClusterController}, but for execution in BYON mode
 * ("bring your own nodes").
 */
public class ByonClusterController extends ClusterController {

  @Override
  public String getName() {
    return "byon";
  }

  public Cluster launchCluster(ClusterSpec clusterSpec) throws IOException,
      InterruptedException {

    LoadingCache<String, ClusterActionHandler> handlerMap = handlerMapFactory
        .create();

    ClusterAction bootstrapper = new ByonClusterAction(BOOTSTRAP_ACTION, getCompute(), handlerMap);
    Cluster cluster = bootstrapper.execute(clusterSpec, null);

    ClusterAction configurer = new ByonClusterAction(CONFIGURE_ACTION, getCompute(), handlerMap);
    cluster = configurer.execute(clusterSpec, cluster);

    getClusterStateStore(clusterSpec).save(cluster);
    
    return cluster;
  }

  public void destroyCluster(ClusterSpec clusterSpec) throws IOException,
      InterruptedException {
    LoadingCache<String, ClusterActionHandler> handlerMap = handlerMapFactory
        .create();

    ClusterAction destroyer = new ByonClusterAction(DESTROY_ACTION, getCompute(), handlerMap);
    destroyer.execute(clusterSpec, null);
  }
  
  public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(final ClusterSpec spec,
      Predicate<NodeMetadata> condition, final Statement statement) throws IOException, RunScriptOnNodesException {
    
    ComputeServiceContext computeServiceContext = getCompute().apply(spec);
    ComputeService computeService = computeServiceContext.getComputeService();
    Cluster cluster = getClusterStateStore(spec).load();

    RunScriptOptions options = RunScriptOptions.Builder.runAsRoot(false).wrapInInitScript(false);
    return computeService.runScriptOnNodesMatching(Predicates.<NodeMetadata>and(condition, runningIn(cluster)), statement, options);
  }

  private Predicate<NodeMetadata> runningIn(Cluster cluster) {
    final Set<String> instanceIds = new HashSet<String>(Collections2.transform(cluster.getInstances(), new Function<Instance, String>() {
      @Override
      public String apply(Instance instance) {
        return instance.getId();
      }
    }));
    return new Predicate<NodeMetadata>() {
      @Override
      public boolean apply(final NodeMetadata nodeMetadata) {
        return instanceIds.contains(nodeMetadata.getId()) && nodeMetadata.getState().equals(NodeState.RUNNING);
      }
    };
  }

  @Override
  public void destroyInstance(ClusterSpec clusterSpec, String instanceId)
      throws IOException {
    // TODO
  }
  
  @Override
  public Set<? extends NodeMetadata> getNodes(ClusterSpec clusterSpec)
      throws IOException, InterruptedException {
    ComputeServiceContext computeServiceContext = getCompute().apply(clusterSpec);
    ComputeService computeService = computeServiceContext.getComputeService();
    return computeService.listNodesDetailsMatching(Predicates.in(computeService.listNodes()));
  }
}
