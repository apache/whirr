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

package org.apache.whirr.compute;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class StartupProcess implements Callable<Set<? extends NodeMetadata>> {

  private static final Logger LOG =
    LoggerFactory.getLogger(StartupProcess.class);

  final private String clusterName;
  final private int numberOfNodes;
  final private int minNumberOfNodes;
  final private int maxStartupRetries;
  final private Set<String> roles;
  final private ComputeService computeService;
  final private Template template;
  final private ExecutorService executorService;
  final private NodeStarterFactory starterFactory;

  private Set<NodeMetadata> successfulNodes = Sets.newLinkedHashSet();
  private Map<NodeMetadata, Throwable> lostNodes = Maps.newHashMap();

  private Future<Set<NodeMetadata>> nodesFuture;

  public StartupProcess(final String clusterName, final int numberOfNodes,
                        final int minNumberOfNodes, final int maxStartupRetries, final Set<String> roles,
                        final ComputeService computeService, final Template template,
                        final ExecutorService executorService, final NodeStarterFactory starterFactory) {
    this.clusterName = clusterName;
    this.numberOfNodes = numberOfNodes;
    this.minNumberOfNodes = minNumberOfNodes;
    this.maxStartupRetries = maxStartupRetries;
    this.roles = roles;
    this.computeService = computeService;
    this.template = template;
    this.executorService = executorService;
    this.starterFactory = starterFactory;
  }

  @Override
  public Set<? extends NodeMetadata> call() throws Exception {
    int retryCount = 0;
    boolean retryRequired;
    try {
    do {
        runNodesWithTag();
        waitForOutcomes();
        retryRequired = !isDone();

        if (++retryCount > maxStartupRetries) {
          break; // no more retries
        }
      } while (retryRequired);

      if (retryRequired) {// if still required, we cannot use the cluster
        // in this case of failed cluster startup, cleaning of the nodes are postponed
        throw new IOException("Too many instance failed while bootstrapping! "
            + successfulNodes.size() + " successfully started instances while " + lostNodes.size() + " instances failed");
      }
    } finally {
      cleanupFailedNodes();
    }
    return successfulNodes;
  }

  String getClusterName() {
    return clusterName;
  }

  Template getTemplate() {
    return template;
  }

  Set<NodeMetadata> getSuccessfulNodes() {
    return successfulNodes;
  }

  Map<NodeMetadata, Throwable> getNodeErrors() {
    return lostNodes;
  }

  boolean isDone() {
    return successfulNodes.size() >= minNumberOfNodes;
  }

  void runNodesWithTag() {
    final int num = numberOfNodes - successfulNodes.size();
    this.nodesFuture = executorService.submit(starterFactory.create(
        computeService, clusterName, roles, num, template));
  }

  void waitForOutcomes() throws InterruptedException {
    try {
      Set<? extends NodeMetadata> nodes = nodesFuture.get();
      successfulNodes.addAll(nodes);
    } catch (ExecutionException e) {
      // checking RunNodesException and collect the outcome
      Throwable th = e.getCause();
      if (th instanceof RunNodesException) {
        RunNodesException rnex = (RunNodesException) th;
        successfulNodes.addAll(rnex.getSuccessfulNodes());
        lostNodes.putAll(rnex.getNodeErrors());
      } else {
        LOG.error("Unexpected error while starting " + numberOfNodes + " nodes, minimum "
            + minNumberOfNodes + " nodes for " + roles + " of cluster " + clusterName, e);
      }
    }
  }

  void cleanupFailedNodes() throws InterruptedException {
    if (lostNodes.size() > 0) {
      // parallel destroy of failed nodes
      Set<Future<NodeMetadata>> deletingNodeFutures = Sets.newLinkedHashSet();
      Iterator<?> it = lostNodes.keySet().iterator();
      while (it.hasNext()) {
        final NodeMetadata badNode = (NodeMetadata) it.next();
        deletingNodeFutures.add(executorService.submit(
            new Callable<NodeMetadata>() {
              public NodeMetadata call() throws Exception {
                final String nodeId = badNode.getId();
                LOG.info("Deleting failed node node {}", nodeId);
                computeService.destroyNode(nodeId);
                LOG.info("Node deleted: {}", nodeId);
                return badNode;
              }
            }
          ));
      }
      Iterator<Future<NodeMetadata>> results = deletingNodeFutures.iterator();
      while (results.hasNext()) {
        try {
          results.next().get();
        } catch (ExecutionException e) {
          LOG.warn("Error while destroying failed node:", e);
        }
      }
    }
  }
}
