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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ByonClusterAction extends ScriptBasedClusterAction {

  private static final Logger LOG =
    LoggerFactory.getLogger(ByonClusterAction.class);

  private final String action;
  
  public ByonClusterAction(String action, Function<ClusterSpec, ComputeServiceContext> getCompute,
      Map<String, ClusterActionHandler> handlerMap) {
    super(getCompute, handlerMap);
    this.action = action;
  }
  
  @Override
  protected String getAction() {
    return action;
  }
  
  @Override
  protected void doAction(Map<InstanceTemplate, ClusterActionEvent> eventMap)
      throws IOException, InterruptedException {
    
    ExecutorService executorService = Executors.newCachedThreadPool();
    
    Set<Future<Void>> futures = Sets.newHashSet();

    List<NodeMetadata> nodes = Lists.newArrayList();
    int numberAllocated = 0;
    Set<Instance> allInstances = Sets.newLinkedHashSet();

    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap.entrySet()) {

      final ClusterSpec clusterSpec = entry.getValue().getClusterSpec();
      final StatementBuilder statementBuilder = entry.getValue().getStatementBuilder();

      final ComputeServiceContext computeServiceContext = getCompute().apply(clusterSpec);
      final ComputeService computeService = computeServiceContext.getComputeService();

      Credentials credentials = new Credentials(clusterSpec.getIdentity(), clusterSpec.getCredential());
      
      if (numberAllocated == 0) {
        for (ComputeMetadata compute : computeService.listNodes()) {
          if (!(compute instanceof NodeMetadata)) {
            throw new IllegalArgumentException("Not an instance of NodeMetadata: " + compute);
          }
          nodes.add((NodeMetadata) compute);
        }
      }

      int num = entry.getKey().getNumberOfInstances();
      final List<NodeMetadata> templateNodes =
        nodes.subList(numberAllocated, numberAllocated + num);
      numberAllocated += num;
      
      final Set<Instance> templateInstances = getInstances(
          credentials, entry.getKey().getRoles(), templateNodes
      );
      allInstances.addAll(templateInstances);
      
      for (final Instance instance : templateInstances) {
        final Statement statement = statementBuilder.build(clusterSpec, instance);

        futures.add(executorService.submit(new Callable<Void>() {
          @Override
          public Void call() throws Exception {
            LOG.info("Running script on: {}", instance.getId());

            if (LOG.isDebugEnabled()) {
              LOG.debug("Running script:\n{}", statement.render(OsFamily.UNIX));
            }

            computeService.runScriptOnNode(instance.getId(), statement);
            LOG.info("Script run completed on: {}", instance.getId());

            return null;
          }
        }));
      }
    }
    
    for (Future<Void> future : futures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    }
      
    if (action.equals(ClusterActionHandler.BOOTSTRAP_ACTION)) {
      Cluster cluster = new Cluster(allInstances);
      for (ClusterActionEvent event : eventMap.values()) {
        event.setCluster(cluster);
      }
    }
  }
  
  private Set<Instance> getInstances(final Credentials credentials, final Set<String> roles,
      Collection<NodeMetadata> nodes) {
    return Sets.newLinkedHashSet(Collections2.transform(Sets.newLinkedHashSet(nodes),
        new Function<NodeMetadata, Instance>() {
          @Override
          public Instance apply(NodeMetadata node) {
            String publicIp = Iterables.get(node.getPublicAddresses(), 0);
            return new Instance(
                credentials, roles, publicIp, publicIp, node.getId(), node
            );
          }
        }
    ));
  }
  
}
