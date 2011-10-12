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
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.FirewallManager.Rule;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.domain.Credentials;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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

import static org.apache.whirr.RolePredicates.onlyRolesIn;
import static org.jclouds.compute.options.RunScriptOptions.Builder.overrideCredentialsWith;

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
      throws IOException, InterruptedException {

    final ExecutorService executorService = Executors.newCachedThreadPool();
    final Collection<Future<ExecResponse>> futures = Sets.newHashSet();

    for (Entry<InstanceTemplate, ClusterActionEvent> entry : eventMap.entrySet()) {
      applyFirewallRules(entry.getValue());
      
      ClusterSpec clusterSpec = entry.getValue().getClusterSpec();
      Cluster cluster = entry.getValue().getCluster();

      StatementBuilder statementBuilder = entry.getValue().getStatementBuilder();

      ComputeServiceContext computeServiceContext = getCompute().apply(clusterSpec);
      final ComputeService computeService = computeServiceContext.getComputeService();

      final Credentials credentials = new Credentials(
          clusterSpec.getClusterUser(),
          clusterSpec.getPrivateKey()
      );

      Set<Instance> instances = cluster.getInstancesMatching(
          onlyRolesIn(entry.getKey().getRoles()));

      String instanceIds = Joiner.on(", ").join(Iterables.transform(instances,
          new Function<Instance, String>() {
            @Override
            public String apply(@Nullable Instance instance) {
              return instance == null ? "<null>" : instance.getId();
            }
          })
      );

      LOG.info("Starting to run configuration scripts on cluster " +
          "instances: {}", instanceIds);

      for (final Instance instance : instances) {
        final Statement statement = statementBuilder.build(clusterSpec, instance);

        futures.add(executorService.submit(new Callable<ExecResponse>() {
          @Override
          public ExecResponse call() {

            LOG.info("Running configuration script on: {}", instance.getId());
            if (LOG.isDebugEnabled()) {
              LOG.debug("Configuration script for {}:\n{}", instance.getId(),
                statement.render(OsFamily.UNIX));
            }

            try {
              return computeService.runScriptOnNode(
                instance.getId(),
                statement,
                overrideCredentialsWith(credentials).runAsRoot(true)
              );

            } finally {
              LOG.info("Configuration script run completed on: {}", instance.getId());
            }
          }
        }));
      }
    }

    for (Future<ExecResponse> future : futures) {
      try {
        ExecResponse execResponse = future.get();
        if (execResponse.getExitCode() != 0) {
          LOG.error("Error running script: {}\n{}", execResponse.getError(),
              execResponse.getOutput());
        }
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    }

    LOG.info("Finished running configuration scripts on all cluster instances");
  }

  /**
   * Apply the firewall rules specified via configuration.
   */
  private void applyFirewallRules(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    
    Map<String, List<String>> firewallRules = clusterSpec.getFirewallRules();
    for (String role: firewallRules.keySet()) {
      Rule rule = Rule.create();
      
      if (role == null) {
        rule.destination(event.getCluster().getInstances());
      } else {
        rule.destination(RolePredicates.role(role));
      }
      
      List<String> ports = firewallRules.get(role);
      rule.ports(Ints.toArray(Collections2.transform(ports, new Function<String,Integer>() {
        @Override
        public Integer apply(String input) {
          return Integer.valueOf(input);
        }
      })));

      event.getFirewallManager().addRule(rule);
    }
  }

}
