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

import static org.apache.whirr.RolePredicates.withIds;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideLoginCredentials;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.whirr.actions.BootstrapClusterAction;
import org.apache.whirr.actions.CleanupClusterAction;
import org.apache.whirr.actions.ConfigureServicesAction;
import org.apache.whirr.actions.DestroyClusterAction;
import org.apache.whirr.actions.StartServicesAction;
import org.apache.whirr.actions.StopServicesAction;
import org.apache.whirr.state.ClusterStateStore;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.apache.whirr.service.ComputeCache;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;


/**
 * This class is used to start and stop clusters.
 */
public class ClusterController {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterController.class);

  private static final ImmutableSet<String> EMPTYSET = ImmutableSet.of();

  private final Function<ClusterSpec, ComputeServiceContext> getCompute;
  private final ClusterStateStoreFactory stateStoreFactory;

  protected HandlerMapFactory handlerMapFactory = new HandlerMapFactory();


  public ClusterController() {
    this(ComputeCache.INSTANCE, new ClusterStateStoreFactory());
  }

  public ClusterController(Function<ClusterSpec, ComputeServiceContext> getCompute,
                           ClusterStateStoreFactory stateStoreFactory) {
    this.getCompute = getCompute;
    this.stateStoreFactory = stateStoreFactory;
  }

  /**
   * @return the unique name of the service.
   */
  public String getName() {
    throw new UnsupportedOperationException("No service name");
  }

  /**
   * @return compute service contexts for use in managing the service
   */
  public Function<ClusterSpec, ComputeServiceContext> getCompute() {
    return getCompute;
  }

  /**
   * Start the cluster described by <code>clusterSpec</code> and block until the
   * cluster is
   * available. It is not guaranteed that the service running on the cluster
   * has started when this method returns.
   *
   * @param clusterSpec
   * @return an object representing the running cluster
   * @throws IOException          if there is a problem while starting the cluster. The
   *                              cluster may or may not have started.
   * @throws InterruptedException if the thread is interrupted.
   */
  public Cluster launchCluster(ClusterSpec clusterSpec)
    throws IOException, InterruptedException {
    try {
      Cluster cluster = bootstrapCluster(clusterSpec);
      cluster = configureServices(clusterSpec, cluster);
      return startServices(clusterSpec, cluster);

    } catch (Throwable e) {

      if (clusterSpec.isTerminateAllOnLaunchFailure()) {
        LOG.error("Unable to start the cluster. Terminating all nodes.", e);
        destroyCluster(clusterSpec);

      } else {
        LOG.error("*CRITICAL* the cluster failed to launch and the automated node termination" +
          " option was not selected, there might be orphaned nodes.", e);
      }

      throw new RuntimeException(e);
    }
  }

  /**
   * Provision the hardware resources needed for running services
   */
  public Cluster bootstrapCluster(ClusterSpec clusterSpec) throws IOException, InterruptedException {
    BootstrapClusterAction bootstrapper = new BootstrapClusterAction(getCompute(), handlerMapFactory.create());
    Cluster cluster = bootstrapper.execute(clusterSpec, null);
    getClusterStateStore(clusterSpec).save(cluster);
    return cluster;
  }

  /**
   * Configure cluster services
   */
  @Beta
  public Cluster configureServices(ClusterSpec spec) throws IOException, InterruptedException {
    return configureServices(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  @Beta
  public Cluster configureServices(ClusterSpec clusterSpec, Cluster cluster)
    throws IOException, InterruptedException {
    return configureServices(clusterSpec, cluster, EMPTYSET, EMPTYSET);
  }

  @Beta
  public Cluster configureServices(ClusterSpec clusterSpec, Cluster cluster, Set<String> targetRoles,
        Set<String> targetInstanceIds) throws IOException, InterruptedException {
    ConfigureServicesAction configurer = new ConfigureServicesAction(getCompute(), handlerMapFactory.create(),
        targetRoles, targetInstanceIds);
    return configurer.execute(clusterSpec, cluster);
  }

  /**
   * Start the cluster services
   */
  public Cluster startServices(ClusterSpec spec) throws IOException, InterruptedException {
    return startServices(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  public Cluster startServices(ClusterSpec clusterSpec, Cluster cluster)
    throws IOException, InterruptedException {
    return startServices(clusterSpec, cluster, EMPTYSET, EMPTYSET);
  }
  
  public Cluster startServices(ClusterSpec clusterSpec, Cluster cluster,
      Set<String> targetRoles, Set<String> targetInstanceIds) throws IOException, InterruptedException {
    StartServicesAction starter = new StartServicesAction(getCompute(), handlerMapFactory.create(), targetRoles, targetInstanceIds);
    return starter.execute(clusterSpec, cluster);
  }

  /**
   * Stop the cluster services
   */
  public Cluster stopServices(ClusterSpec spec) throws IOException, InterruptedException {
    return stopServices(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  public Cluster stopServices(ClusterSpec clusterSpec, Cluster cluster)
    throws IOException, InterruptedException {
    return stopServices(clusterSpec, cluster, EMPTYSET, EMPTYSET);

  }
  
  public Cluster stopServices(ClusterSpec clusterSpec, Cluster cluster, Set<String> targetRoles,
    Set<String> targetInstanceIds) throws IOException, InterruptedException {
    StopServicesAction stopper = new StopServicesAction(getCompute(), handlerMapFactory.create(), targetRoles, targetInstanceIds);
    return stopper.execute(clusterSpec, cluster);
  }

  /**
   * Remove the cluster services
   */
  @Beta
  public Cluster cleanupCluster(ClusterSpec spec) throws IOException, InterruptedException {
    return cleanupCluster(spec, new Cluster(getInstances(spec, getClusterStateStore(spec))));
  }

  @Beta
  public Cluster cleanupCluster(ClusterSpec clusterSpec, Cluster cluster)
    throws IOException, InterruptedException {
    CleanupClusterAction cleanner = new CleanupClusterAction(getCompute(), handlerMapFactory.create());
    return cleanner.execute(clusterSpec, cluster);
  }
  
  /**
   * Stop the cluster and destroy all resources associated with it.
   *
   * @throws IOException          if there is a problem while stopping the cluster. The cluster may
   *                              or may not have been stopped.
   * @throws InterruptedException if the thread is interrupted.
   */
  public void destroyCluster(ClusterSpec clusterSpec)
    throws IOException, InterruptedException {
    ClusterStateStore stateStore = getClusterStateStore(clusterSpec);
    destroyCluster(clusterSpec, stateStore.tryLoadOrEmpty());
    stateStore.destroy();
  }

  public void destroyCluster(ClusterSpec clusterSpec, Cluster cluster)
    throws IOException, InterruptedException {
    DestroyClusterAction destroyer = new DestroyClusterAction(getCompute(), handlerMapFactory.create());
    destroyer.execute(clusterSpec, cluster);
  }

  public void destroyInstance(ClusterSpec clusterSpec, String instanceId) throws IOException {
    LOG.info("Destroying instance {}", instanceId);

    /* Destroy the instance */
    ComputeService computeService = getCompute().apply(clusterSpec).getComputeService();
    computeService.destroyNode(instanceId);

    /* .. and update the cluster state storage */
    ClusterStateStore store = getClusterStateStore(clusterSpec);
    Cluster cluster = store.load();
    cluster.removeInstancesMatching(withIds(instanceId));
    store.save(cluster);

    LOG.info("Instance {} destroyed", instanceId);
  }

  public ClusterStateStore getClusterStateStore(ClusterSpec clusterSpec) {
    return stateStoreFactory.create(clusterSpec);
  }

  public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(ClusterSpec spec,
      Predicate<NodeMetadata> condition, Statement statement) throws IOException, RunScriptOnNodesException {
    return runScriptOnNodesMatching(spec, condition, statement, null);
  }

  public Map<? extends NodeMetadata, ExecResponse> runScriptOnNodesMatching(
    ClusterSpec spec, Predicate<NodeMetadata> condition, Statement statement,
    RunScriptOptions options) throws IOException, RunScriptOnNodesException {

    LoginCredentials credentials = LoginCredentials.builder()
      .user(spec.getClusterUser()).privateKey(spec.getPrivateKey()).build();

    if (options == null) {
      options = defaultRunScriptOptionsForSpec(spec);
    } else if (options.getLoginUser() == null) {
      options = options.overrideLoginCredentials(credentials);
    }
    condition = Predicates
      .and(runningInGroup(spec.getClusterName()), condition);

    ComputeServiceContext context = getCompute().apply(spec);
    return context.getComputeService().runScriptOnNodesMatching(condition,
      statement, options);
  }

  public RunScriptOptions defaultRunScriptOptionsForSpec(ClusterSpec spec) {
    LoginCredentials credentials = LoginCredentials.builder()
      .user(spec.getClusterUser()).privateKey(spec.getPrivateKey()).build();
    return overrideLoginCredentials(credentials).wrapInInitScript(false)
      .runAsRoot(false);
  }

  @Deprecated
  public Set<? extends NodeMetadata> getNodes(ClusterSpec clusterSpec)
    throws IOException, InterruptedException {
    ComputeService computeService = getCompute().apply(clusterSpec).getComputeService();
    return computeService.listNodesDetailsMatching(
      runningInGroup(clusterSpec.getClusterName()));
  }

  public Set<Cluster.Instance> getInstances(ClusterSpec spec)
    throws IOException, InterruptedException {
    return getInstances(spec, null);
  }

  public Set<Cluster.Instance> getInstances(ClusterSpec spec, ClusterStateStore stateStore)
    throws IOException, InterruptedException {

    Set<Cluster.Instance> instances = Sets.newLinkedHashSet();
    Cluster cluster = (stateStore != null) ? stateStore.load() : null;

    for (NodeMetadata node : getNodes(spec)) {
      instances.add(toInstance(node, cluster, spec));
    }

    return instances;
  }

  public HandlerMapFactory getHandlerMapFactory() {
    return handlerMapFactory;
  }

  public void setHandlerMapFactory(HandlerMapFactory handlerMapFactory) {
    this.handlerMapFactory = handlerMapFactory;
  }

  private Cluster.Instance toInstance(NodeMetadata metadata, Cluster cluster, ClusterSpec spec) {
    Credentials credentials = new Credentials(spec.getClusterUser(), spec.getPrivateKey());

    Set<String> roles = Sets.newHashSet();
    try {
      if (cluster != null) {
        roles = cluster.getInstanceMatching(withIds(metadata.getId())).getRoles();
      }
    } catch (NoSuchElementException e) {
    }

    return new Cluster.Instance(credentials, roles,
      Iterables.getFirst(metadata.getPublicAddresses(), null),
      Iterables.getFirst(metadata.getPrivateAddresses(), null),
      metadata.getId(), metadata);
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
