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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;

public class FirewallManager {

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

  public FirewallManager(ComputeServiceContext computeServiceContext,
      ClusterSpec clusterSpec, Cluster cluster) {
    this.computeServiceContext = computeServiceContext;
    this.clusterSpec = clusterSpec;
    this.cluster = cluster;
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
        cidrs = Lists.newArrayList(FirewallSettings.getOriginatingIp());
      }
    } else {
      cidrs = Lists.newArrayList(rule.source + "/32");
    }

    Iterable<String> instanceIds =
      Iterables.transform(instances, new Function<Instance, String>() {
        @Override
        public String apply(@Nullable Instance instance) {
          return instance == null ? "<null>" : instance.getId();
        }
      });

    LOG.info("Authorizing firewall ingress to {} on ports {} for {}",
        new Object[] { instanceIds, rule.ports, cidrs });

    FirewallSettings.authorizeIngress(computeServiceContext, instances,
        clusterSpec, cidrs, rule.ports);
  }

}
