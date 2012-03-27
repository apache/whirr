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

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.get;
import static com.google.common.collect.Iterables.transform;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideLoginCredentials;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
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
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.options.RunScriptOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ByonClusterAction extends ScriptBasedClusterAction {

  private static final Logger LOG =
    LoggerFactory.getLogger(ByonClusterAction.class);

  private final String action;
  
  public ByonClusterAction(String action, Function<ClusterSpec, ComputeServiceContext> getCompute,
      LoadingCache<String, ClusterActionHandler> handlerMap) {
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
        
    final Collection<Future<ExecResponse>> futures = Sets.newHashSet();

    List<NodeMetadata> nodes = Lists.newArrayList();
    List<NodeMetadata> usedNodes = Lists.newArrayList();
    int numberAllocated = 0;
    Set<Instance> allInstances = Sets.newLinkedHashSet();

    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap.entrySet()) {

      final ClusterSpec clusterSpec = entry.getValue().getClusterSpec();
      final StatementBuilder statementBuilder = entry.getValue().getStatementBuilder();
      if (statementBuilder.isEmpty()) {
        continue; // skip
      }

      final ComputeServiceContext computeServiceContext = getCompute().apply(clusterSpec);
      final ComputeService computeService = computeServiceContext.getComputeService();

      LoginCredentials credentials = LoginCredentials.builder().user(clusterSpec.getClusterUser())
            .privateKey(clusterSpec.getPrivateKey()).build();
      
      final RunScriptOptions options = overrideLoginCredentials(credentials);

      if (numberAllocated == 0) {
        for (ComputeMetadata compute : computeService.listNodes()) {
          if (!(compute instanceof NodeMetadata)) {
            throw new IllegalArgumentException("Not an instance of NodeMetadata: " + compute);
          }
          nodes.add((NodeMetadata) compute);
        }
      }

      int num = entry.getKey().getNumberOfInstances();
      Predicate<NodeMetadata> unused = not(in(usedNodes));
      Predicate<NodeMetadata> instancePredicate = new TagsPredicate(StringUtils.split(entry.getKey().getHardwareId()));      

      List<NodeMetadata> templateNodes = Lists.newArrayList(filter(nodes, and(unused, instancePredicate)));
      if (templateNodes.size() < num) {
        LOG.warn("Not enough nodes available for template " + StringUtils.join(entry.getKey().getRoles(), "+"));
      }
      templateNodes = templateNodes.subList(0, num);
      usedNodes.addAll(templateNodes);
      numberAllocated = usedNodes.size() ;
      
      Set<Instance> templateInstances = getInstances(credentials, entry.getKey().getRoles(), templateNodes);
      allInstances.addAll(templateInstances);
      
      for (final Instance instance : templateInstances) {
         futures.add(runStatementOnInstanceInCluster(statementBuilder, instance, clusterSpec, options));
      }
    }
    
    for (Future<ExecResponse> future : futures) {
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
      Iterable<NodeMetadata> nodes) {
    return ImmutableSet.copyOf(transform(nodes,
        new Function<NodeMetadata, Instance>() {
          @Override
          public Instance apply(NodeMetadata node) {
            String publicIp = get(node.getPublicAddresses(), 0);
            return new Instance(
                credentials, roles, publicIp, publicIp, node.getId(), node
            );
          }
        }
    ));
  }
  
  private static class TagsPredicate implements Predicate<NodeMetadata>{
    private String[] tags;

    public TagsPredicate(String[] tags) {
      this.tags = tags;
    }

    @Override
    public boolean apply(NodeMetadata node) {
      if (tags == null) {
        return true;
      } else {
        return node.getTags().containsAll(Arrays.asList(tags));
      }
    }

  }
}
