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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.whirr.actions.BootstrapClusterAction;
import org.apache.whirr.actions.ConfigureClusterAction;
import org.apache.whirr.actions.DestroyClusterAction;
import org.apache.whirr.service.ClusterStateStore;
import org.apache.whirr.service.ClusterStateStoreFactory;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.ComputeServiceContextFactory;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.whirr.RolePredicates.withIds;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideCredentialsWith;

/**
 * This class is used to start and stop clusters.
 */
public class ClusterController {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterController.class);

  private ClusterStateStoreFactory stateStoreFactory;

  public ClusterController() {
    this(new ClusterStateStoreFactory());
  }

  public ClusterController(ClusterStateStoreFactory stateStoreFactory) {
    this.stateStoreFactory = stateStoreFactory;
  }

  /**
   * @return the unique name of the service.
   */
  public String getName() {
    throw new UnsupportedOperationException("No service name");
  }
  /**
   * Start the cluster described by <code>clusterSpec</code> and block until the
   * cluster is
   * available. It is not guaranteed that the service running on the cluster
   * has started when this method returns.
   * @param clusterSpec
   * @return an object representing the running cluster
   * @throws IOException if there is a problem while starting the cluster. The
   * cluster may or may not have started.
   * @throws InterruptedException if the thread is interrupted.
   */
  public Cluster launchCluster(ClusterSpec clusterSpec)
      throws IOException, InterruptedException {
    
    ComputeServiceContextFactory computeServiceFactory = new ComputeServiceContextFactory();
    Map<String, ClusterActionHandler> handlerMap = new HandlerMapFactory().create();

    BootstrapClusterAction bootstrapper = new BootstrapClusterAction(computeServiceFactory, handlerMap);
    Cluster cluster = bootstrapper.execute(clusterSpec, null);

    ConfigureClusterAction configurer = new ConfigureClusterAction(computeServiceFactory, handlerMap);
    cluster = configurer.execute(clusterSpec, cluster);

    stateStoreFactory.create(clusterSpec).save(cluster);

    return cluster;
  }

  /**
   * Stop the cluster and destroy all resources associated with it.
   *
   * @throws IOException if there is a problem while stopping the cluster. The
   * cluster may or may not have been stopped.
   * @throws InterruptedException if the thread is interrupted.
   */
  public void destroyCluster(ClusterSpec clusterSpec) throws IOException,
      InterruptedException {
    DestroyClusterAction destroyer = new DestroyClusterAction(new ComputeServiceContextFactory());
    destroyer.execute(clusterSpec, null);

    stateStoreFactory.create(clusterSpec).destroy();
  }

  public void destroyInstance(ClusterSpec clusterSpec, String instanceId) throws IOException {
    LOG.info("Destroying instance {}", instanceId);

    /* Destroy the instance */
    ComputeService computeService = ComputeServiceContextBuilder
      .build(clusterSpec).getComputeService();
    computeService.destroyNode(instanceId);

    /* .. and update the cluster state storage */
    ClusterStateStore store = stateStoreFactory.create(clusterSpec);
    Cluster cluster = store.load();
    cluster.removeInstancesMatching(withIds(instanceId));
    store.save(cluster);

    LOG.info("Instance {} destroyed", instanceId);
  }

  public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(ClusterSpec spec,
        Predicate<NodeMetadata> condition, Statement statement) throws IOException, RunScriptOnNodesException {

    Credentials credentials = new Credentials(spec.getClusterUser(), spec.getPrivateKey());
    ComputeServiceContext context = ComputeServiceContextBuilder.build(spec);
    try {
      condition = Predicates.and(runningInGroup(spec.getClusterName()), condition);
      return context.getComputeService().runScriptOnNodesMatching(condition,
        statement, overrideCredentialsWith(credentials).wrapInInitScript(false).runAsRoot(false));
    } finally {
      context.close();
    }
  }
  
  public Set<? extends NodeMetadata> getNodes(ClusterSpec clusterSpec)
    throws IOException, InterruptedException {
    ComputeService computeService =
      ComputeServiceContextBuilder.build(new ComputeServiceContextFactory(), clusterSpec).getComputeService();
    return computeService.listNodesDetailsMatching(
        runningInGroup(clusterSpec.getClusterName()));
  }
  
  public static Predicate<ComputeMetadata> runningInGroup(final String group) {
    return new Predicate<ComputeMetadata>() {
      @Override
      public boolean apply(ComputeMetadata computeMetadata) {
        // Not all list calls return NodeMetadata (e.g. VCloud)
        if (computeMetadata instanceof NodeMetadata) {
          NodeMetadata nodeMetadata = (NodeMetadata) computeMetadata;
          return group.equals(nodeMetadata.getGroup())
            && nodeMetadata.getState() == NodeState.RUNNING;
        }
        return false;
      }
      @Override
      public String toString() {
        return "runningInGroup(" + group + ")";
      }
    };
  }

}
