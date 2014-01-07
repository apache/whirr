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

package org.apache.whirr.service;

import static org.jclouds.scriptbuilder.domain.Statements.exec;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.SecurityGroup;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.javax.annotation.Nullable;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirewallManager {

  public static class StoredRule {
    private Rule rule;
    private List<String> cidrs;
    private Set<Instance> instances;
      
    public StoredRule(Rule rule, List<String> cidrs, Set<Instance> instances) {
      this.rule = rule;
      this.cidrs = cidrs;
      this.instances = instances;
    }

    /**
     * Get the Rule object for this stored rule.
     */
    public Rule rule() {
      return rule;
    }

    /**
     * Get the CIDRs for this stored rule.
     */
    public List<String> cidrs() {
      return cidrs;
    }

    /**
     * Get the set of Instances for this stored rule.
     */
    public Set<Instance> instances() {
      return instances;
    }
  }
    
  public static class Rule {

    public static Rule create() {
      return new Rule();
    }

    private String source;
    private Set<Instance> destinations;
    private Predicate<Instance> destinationPredicate;
    private int[] ports;

    private Rule() {
    }

    /**
     * @param source The allowed source IP for traffic. If not set, this will
     * default to {@link ClusterSpec#getClientCidrs()}, or, if that is not set,
     *  to the client's originating IP.
     */
    public Rule source(String source) {
      this.source = source;
      return this;
    }

    /**
     * @param destination The allowed destination instance.
     */
    public Rule destination(Instance destination) {
      this.destinations = Collections.singleton(destination);
      return this;
    }

    /**
     * @param destinations The allowed destination instances.
     */
    public Rule destination(Set<Instance> destinations) {
      this.destinations = destinations;
      return this;
    }

    /**
     * @param destinationPredicate A predicate which is used to evaluate the
     * allowed destination instances.
     */
    public Rule destination(Predicate<Instance> destinationPredicate) {
      this.destinationPredicate = destinationPredicate;
      return this;
    }

    /**
     * @param port The port on the destination which is to be opened. Overrides
     * any previous calls to {@link #port(int)} or {@link #ports(int...)}.
     */
    public Rule port(int port) {
      this.ports = new int[] { port };
      return this;
    }

    /**
     * @param ports The ports on the destination which are to be opened.
     * Overrides
     * any previous calls to {@link #port(int)} or {@link #ports(int...)}.
     */
    public Rule ports(int... ports) {
      this.ports = ports;
      return this;
    }
  }

  private static final Logger LOG = LoggerFactory
    .getLogger(FirewallManager.class);

  private ComputeServiceContext computeServiceContext;
  private ClusterSpec clusterSpec;
  private Cluster cluster;
  private Set<StoredRule> storedRules;
    
  public FirewallManager(ComputeServiceContext computeServiceContext,
                         ClusterSpec clusterSpec, Cluster cluster) {
    this.computeServiceContext = computeServiceContext;
    this.clusterSpec = clusterSpec;
    this.cluster = cluster;
    this.storedRules = Sets.newHashSet();
  }

  public void addRules(Rule... rules) throws IOException {
    for (Rule rule : rules) {
      addRule(rule);
    }
  }

  public void addRules(Set<Rule> rules) throws IOException {
    for (Rule rule : rules) {
      addRule(rule);
    }
  }

  /**
   * Rules are additive. If no source is set then it will default
   * to {@link ClusterSpec#getClientCidrs()}, or, if that is not set,
   * to the client's originating IP. If no destinations or ports
   * are set then the rule has not effect.
   *
   * @param rule The rule to add to the firewall.
   * @throws IOException
   */
  public void addRule(Rule rule) throws IOException {
    Set<Instance> instances = Sets.newHashSet();
    if (rule.destinations != null) {
      instances.addAll(rule.destinations);
    }
    if (rule.destinationPredicate != null) {
      instances.addAll(cluster.getInstancesMatching(rule.destinationPredicate));
    }
    List<String> cidrs;
    if (rule.source == null) {
      cidrs = clusterSpec.getClientCidrs();
      if (cidrs == null || cidrs.isEmpty()) {
        cidrs = Lists.newArrayList(getOriginatingIp());
      }
    } else {
      cidrs = Lists.newArrayList(rule.source + "/32");
    }

    storedRules.add(new StoredRule(rule, cidrs, instances));
  }

  /**
   * Logs information about the StoredRule we're adding
   * @param storedRule the StoredRule we're adding
   */
  private void logInstanceRules(StoredRule storedRule) {
    Iterable<String> instanceIds =
      Iterables.transform(storedRule.instances(), new Function<Instance, String>() {
          @Override
          public String apply(@Nullable Instance instance) {
            return instance == null ? "<null>" : instance.getId();
          }
        });
      
      
      
    LOG.info("Authorizing firewall ingress to {} on ports {} for {}",
             new Object[] { instanceIds, storedRule.rule().ports, storedRule.cidrs() });
  }

  /**
   * Authorizes all rules via jclouds security groups interface.
   */
  public void authorizeAllRules() {
    for (StoredRule storedRule : storedRules) { 
      logInstanceRules(storedRule);
      authorizeIngress(computeServiceContext, storedRule.instances(),
                       clusterSpec, storedRule.cidrs(), storedRule.rule().ports);
    }
  }

  /**
   * Returns a list of Statements for executing iptables for the stored rules.
   * @return List of iptables Statements.
   */
  public List<Statement> getRulesAsStatements() {
    List<Statement> ruleStatements = Lists.newArrayList();

    for (StoredRule storedRule : storedRules) {
      logInstanceRules(storedRule);
      for (String cidr : storedRule.cidrs()) {
        for (int port : storedRule.rule().ports) {
          ruleStatements.add(exec(String.format("iptables -I INPUT 1 -p tcp --dport %d --source %s -j ACCEPT || true",
                                                port, cidr)));
        }
      }
    }

    ruleStatements.add(exec("iptables-save || true"));

    return ruleStatements;
  }

  /**
   * @return the IP address of the client on which this code is running.
   * @throws IOException
   */
  private String getOriginatingIp() throws IOException {
    if ("stub".equals(clusterSpec.getProvider())) {
      return "62.217.232.123";
    }

    URL url = new URL("http://checkip.amazonaws.com/");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.connect();
    return IOUtils.toString(connection.getInputStream()).trim() + "/32";
  }

  public static void authorizeIngress(ComputeServiceContext computeServiceContext,
                                      Set<Instance> instances, final ClusterSpec clusterSpec, List<String> cidrs, int... ports) {

    try {
      if (computeServiceContext.getComputeService().getSecurityGroupExtension().isPresent()) {
        SecurityGroupExtension securityGroupExtension = computeServiceContext.getComputeService().getSecurityGroupExtension().get();
        Instance instance = Iterables.getFirst(instances, null);
        if (instance != null) {
          SecurityGroup group = Iterables.find(securityGroupExtension.listSecurityGroupsForNode(instance.getNodeMetadata().getId()),
              new Predicate<SecurityGroup>() {
                @Override
                public boolean apply(SecurityGroup input) {
                  if (input.getName().contains(clusterSpec.getClusterName()) ||
                      input.getId().contains(clusterSpec.getClusterName())) {
                    return true;
                  }
                  return false;  //To change body of implemented methods use File | Settings | File Templates.
                }
              });

          if (group == null) {
            group = securityGroupExtension.createSecurityGroup(clusterSpec.getClusterName(),
                instance.getNodeMetadata().getLocation());
          }

          for (int port : ports) {
            IpPermission.Builder builder = IpPermission.builder();
            builder.cidrBlocks(cidrs);
            builder.ipProtocol(IpProtocol.TCP);
            builder.fromPort(port);
            builder.toPort(port);
            securityGroupExtension.addIpPermission(builder.build(), group);
          }
        } else {
          LOG.warn("Cannot find any instance for group, so cannot determine security group.");
        }
      } else {
        LOG.warn("No security group extension present for provider, so cannot set up security group.");
      }
    } catch (Exception e) {
      LOG.error("Error setting up security groups: {}", e);
    }
  }
}
