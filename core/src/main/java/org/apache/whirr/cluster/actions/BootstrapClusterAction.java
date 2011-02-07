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

import static org.jclouds.compute.options.TemplateOptions.Builder.*;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.scriptbuilder.domain.AuthorizeRSAPublicKey;
import org.jclouds.scriptbuilder.domain.InstallRSAPrivateKey;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link org.apache.whirr.service.ClusterAction} that starts instances in a cluster in parallel and
 * runs bootstrap scripts on them.
 */
public class BootstrapClusterAction extends ScriptBasedClusterAction {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(BootstrapClusterAction.class);
  
  @Override
  protected String getAction() {
    return ClusterActionHandler.BOOTSTRAP_ACTION;
  }
  
  @Override
  protected void doAction(Map<InstanceTemplate, ClusterActionEvent> eventMap)
      throws IOException, InterruptedException {
    LOG.info("Bootstrapping cluster");
    
    ExecutorService executorService = Executors.newCachedThreadPool();
    
    Map<InstanceTemplate, Future<Set<? extends NodeMetadata>>> futures =
      Maps.newHashMap();
    
    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap.entrySet()) {
      final InstanceTemplate instanceTemplate = entry.getKey();
      final ClusterSpec clusterSpec = entry.getValue().getClusterSpec();
      StatementBuilder statementBuilder = entry.getValue().getStatementBuilder();
      ComputeServiceContext computeServiceContext =
        ComputeServiceContextBuilder.build(clusterSpec);
      final ComputeService computeService =
        computeServiceContext.getComputeService();
      final Template template = buildTemplate(clusterSpec, computeService,
          statementBuilder, entry.getValue().getTemplateBuilderStrategy());
      
      Future<Set<? extends NodeMetadata>> nodesFuture = executorService.submit(
        new Callable<Set<? extends NodeMetadata>>() {
          public Set<? extends NodeMetadata> call() throws Exception {
            int num = instanceTemplate.getNumberOfInstances();
            LOG.info("Starting {} node(s) with roles {}", num,
                instanceTemplate.getRoles());
            Set<? extends NodeMetadata> nodes = computeService
                .createNodesInGroup(clusterSpec.getClusterName(), num, template);
            LOG.info("Nodes started: {}", nodes);
            return nodes;
          }
        }
      );
      futures.put(instanceTemplate, nodesFuture);
    }
    
    Set<Instance> instances = Sets.newLinkedHashSet();
    for (Entry<InstanceTemplate, Future<Set<? extends NodeMetadata>>> entry :
        futures.entrySet()) {
      Set<? extends NodeMetadata> nodes;
      try {
        nodes = entry.getValue().get();
      } catch (ExecutionException e) {
        // TODO: Check for RunNodesException and don't bail out if only a few
        // have failed to start
        throw new IOException(e);
      }
      Set<String> roles = entry.getKey().getRoles();
      instances.addAll(getInstances(roles, nodes));
    }
    Cluster cluster = new Cluster(instances);
    for (ClusterActionEvent event : eventMap.values()) {
      event.setCluster(cluster);
    }
  }
  
  private Template buildTemplate(ClusterSpec clusterSpec,
      ComputeService computeService, StatementBuilder statementBuilder,
      TemplateBuilderStrategy strategy)
      throws MalformedURLException {
    LOG.info("Configuring template");
    if (LOG.isDebugEnabled())
      LOG.debug("Running script:\n{}", statementBuilder.render(OsFamily.UNIX));
    Statement runScript = new StatementList(
          new AuthorizeRSAPublicKey(clusterSpec.getPublicKey()),
          statementBuilder,
          new InstallRSAPrivateKey(clusterSpec.getPrivateKey()));
    TemplateBuilder templateBuilder = computeService.templateBuilder()
      .options(runScript(runScript));
    strategy.configureTemplateBuilder(clusterSpec, templateBuilder);
    return templateBuilder.build();
  }

  private Set<Instance> getInstances(final Set<String> roles,
      Set<? extends NodeMetadata> nodes) {
    return Sets.newLinkedHashSet(Collections2.transform(Sets.newLinkedHashSet(nodes),
        new Function<NodeMetadata, Instance>() {
      @Override
      public Instance apply(NodeMetadata node) {
        try {
        return new Instance(node.getCredentials(), roles,
            InetAddress.getByName(Iterables.get(node.getPublicAddresses(), 0)),
            InetAddress.getByName(Iterables.get(node.getPrivateAddresses(), 0)),
            node.getId());
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
      }
    }));
  }
  
}
