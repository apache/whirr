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
import static org.apache.whirr.RolePredicates.onlyRolesIn;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideCredentialsWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterAction;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ComputationException;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * A {@link ClusterAction} that provides the base functionality for running
 * scripts on instances in the cluster.
 */
public abstract class ScriptBasedClusterAction extends ClusterAction {

  private static final Logger LOG = LoggerFactory
      .getLogger(ScriptBasedClusterAction.class);

  private final Map<String, ClusterActionHandler> handlerMap;

  protected ScriptBasedClusterAction(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      final Map<String, ClusterActionHandler> handlerMap) {
    super(getCompute);
    this.handlerMap = checkNotNull(handlerMap, "handlerMap");
  }

  public Cluster execute(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException, InterruptedException {

    Map<InstanceTemplate, ClusterActionEvent> eventMap = Maps.newHashMap();
    Cluster newCluster = cluster;
    for (InstanceTemplate instanceTemplate : clusterSpec.getInstanceTemplates()) {
      StatementBuilder statementBuilder = new StatementBuilder();

      ComputeServiceContext computeServiceContext = getCompute().apply(
          clusterSpec);
      FirewallManager firewallManager = new FirewallManager(
          computeServiceContext, clusterSpec, newCluster);

      ClusterActionEvent event = new ClusterActionEvent(getAction(),
          clusterSpec, instanceTemplate, newCluster, statementBuilder,
          getCompute(), firewallManager);

      eventMap.put(instanceTemplate, event);
      for (String role : instanceTemplate.getRoles()) {
        safeGetActionHandler(role).beforeAction(event);
      }

      // cluster may have been updated by handler
      newCluster = event.getCluster();
    }

    doAction(eventMap);

    // cluster may have been updated by action
    newCluster = Iterables.get(eventMap.values(), 0).getCluster();

    for (InstanceTemplate instanceTemplate : clusterSpec.getInstanceTemplates()) {
      ClusterActionEvent event = eventMap.get(instanceTemplate);
      for (String role : instanceTemplate.getRoles()) {
        event.setCluster(newCluster);
        safeGetActionHandler(role).afterAction(event);

        // cluster may have been updated by handler
        newCluster = event.getCluster();
      }
    }

    return newCluster;
  }

  protected void doAction(Map<InstanceTemplate, ClusterActionEvent> eventMap)
      throws InterruptedException, IOException {
    runScripts(eventMap);
    postRunScriptsActions(eventMap);
  }

  protected void runScripts(Map<InstanceTemplate, ClusterActionEvent> eventMap)
      throws InterruptedException, IOException {

    final String phaseName = getAction();
    final ExecutorService executorService = Executors.newCachedThreadPool();
    final Collection<Future<ExecResponse>> futures = Sets.newHashSet();

    ClusterSpec clusterSpec = eventMap.values().iterator().next()
        .getClusterSpec();

    ComputeServiceContext computeServiceContext = getCompute().apply(
        clusterSpec);
    final ComputeService computeService = computeServiceContext
        .getComputeService();

    final Credentials credentials = new Credentials(
        clusterSpec.getClusterUser(), clusterSpec.getPrivateKey());

    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap
        .entrySet()) {

      eventSpecificActions(entry);

      Cluster cluster = entry.getValue().getCluster();

      StatementBuilder statementBuilder = entry.getValue()
          .getStatementBuilder();

      Set<Instance> instances = cluster.getInstancesMatching(onlyRolesIn(entry
          .getKey().getRoles()));

      String instanceIds = Joiner.on(", ").join(
          Iterables.transform(instances, new Function<Instance, String>() {
            @Override
            public String apply(@Nullable Instance instance) {
              return instance == null ? "<null>" : instance.getId();
            }
          }));

      LOG.info("Starting to run scripts on cluster for phase {}"
          + "instances: {}", phaseName, instanceIds);

      for (final Instance instance : instances) {
        final Statement statement = statementBuilder.build(clusterSpec,
            instance);

        futures.add(executorService.submit(new Callable<ExecResponse>() {
          @Override
          public ExecResponse call() {

            LOG.info("Running {} phase script on: {}", phaseName,
                instance.getId());
            if (LOG.isDebugEnabled()) {
              LOG.debug("{} phase script on: {}\n{}", new Object[] { phaseName,
                  instance.getId(), statement.render(OsFamily.UNIX) });
            }

            try {
              return computeService.runScriptOnNode(
                  instance.getId(),
                  statement,
                  overrideCredentialsWith(credentials).runAsRoot(true)
                      .nameTask(
                          phaseName + "-"
                              + Joiner.on('_').join(instance.getRoles())));
            } finally {
              LOG.info("{} phase script run completed on: {}", phaseName,
                  instance.getId());
            }
          }
        }));
      }
    }

    for (Future<ExecResponse> future : futures) {
      try {
        ExecResponse execResponse = future.get();
        if (execResponse.getExitCode() != 0) {
          LOG.error("Error running " + phaseName + " script: {}", execResponse);
        } else {
          LOG.info("Successfully executed {} script: {}", phaseName, execResponse);
        }
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    }

    executorService.shutdown();
    LOG.info("Finished running {} phase scripts on all cluster instances",
        phaseName);
  }

  protected void eventSpecificActions(
      Entry<InstanceTemplate, ClusterActionEvent> entry) throws IOException {
  }

  protected void postRunScriptsActions(
      Map<InstanceTemplate, ClusterActionEvent> eventMap) throws IOException {
  }

  /**
   * Try to get an {@see ClusterActionHandler } instance or throw an
   * IllegalArgumentException if not found for this role name
   */
  private ClusterActionHandler safeGetActionHandler(String role) {
    try {
      ClusterActionHandler handler = handlerMap.get(role);
      if (handler == null) {
        throw new IllegalArgumentException("No handler for role " + role);
      }
      return handler;

    } catch (ComputationException e) {
      throw new IllegalArgumentException(e.getCause());
    }
  }

}
