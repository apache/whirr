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

package org.apache.whirr.service.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.jclouds.compute.domain.OsFamily.UBUNTU;
import static org.jclouds.compute.options.TemplateOptions.Builder.runScript;
import static org.jclouds.compute.predicates.NodePredicates.runningWithTag;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.RunUrlBuilder;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.Architecture;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.ssh.ExecResponse;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class CassandraService extends Service {

  public static final String CASSANDRA_ROLE = "cassandra";
  public static final int CLIENT_PORT = 9160;

  @Override
  public String getName() {
    return "cassandra";
  }

  @Override
  public Cluster launchCluster(ClusterSpec clusterSpec) throws IOException {
    ComputeServiceContext computeServiceContext =
        ComputeServiceContextBuilder.build(clusterSpec);
    ComputeService computeService = computeServiceContext.getComputeService();

    byte[] bootScript = RunUrlBuilder.runUrls(clusterSpec.getRunUrlBase(),
        "sun/java/install",
        "apache/cassandra/install");

    TemplateBuilder templateBuilder = computeService.templateBuilder()
        .osFamily(UBUNTU).options(
            runScript(bootScript).installPrivateKey(
                clusterSpec.readPrivateKey()).authorizePublicKey(
                clusterSpec.readPublicKey()));

    // TODO extract this logic elsewhere
    if (clusterSpec.getProvider().equals("ec2"))
      templateBuilder.imageNameMatches(".*10\\.?04.*").osDescriptionMatches(
          "^ubuntu-images.*").architecture(Architecture.X86_32);

    Template template = templateBuilder.build();

    InstanceTemplate instanceTemplate = clusterSpec
        .getInstanceTemplate(CASSANDRA_ROLE);
    checkNotNull(instanceTemplate);
    int clusterSize = instanceTemplate.getNumberOfInstances();
    Set<? extends NodeMetadata> nodeMap;
    try {
      nodeMap = computeService.runNodesWithTag(clusterSpec.getClusterName(),
          clusterSize, template);
    } catch (RunNodesException e) {
      // TODO: can we do better here
      throw new IOException(e);
    }
    
    FirewallSettings.authorizeIngress(computeServiceContext, nodeMap, clusterSpec, CLIENT_PORT);

    List<NodeMetadata> nodes = Lists.newArrayList(nodeMap);
    List<NodeMetadata> seeds = getSeeds(nodes);

    // Pass list of all servers in cluster to configure script.
    String servers = Joiner.on(' ').join(getPrivateIps(seeds));
    byte[] configureScript = RunUrlBuilder
        .runUrls(clusterSpec.getRunUrlBase(),
            "apache/cassandra/post-configure " + servers);

    try {
      Map<? extends NodeMetadata, ExecResponse> responses = computeService
          .runScriptOnNodesMatching(
              runningWithTag(clusterSpec.getClusterName()), configureScript);
      assert responses.size() > 0 : "no nodes matched "
          + clusterSpec.getClusterName();
    } catch (RunScriptOnNodesException e) {
      // TODO: retry
      throw new IOException(e);
    }

    return new Cluster(getInstances(nodes));
  }

  /**
   * Pick a selection of the nodes that are to become seeds. TODO improve
   * selection method. Right now it picks 20% of the nodes as seeds, or a
   * minimum of one node if it is a small cluster.
   * 
   * @param nodes
   *          all nodes in cluster
   * @return list of seeds
   */
  protected List<NodeMetadata> getSeeds(List<NodeMetadata> nodes) {
    int seeds = (int) Math.ceil(Math.max(1, nodes.size() * 0.2));
    List<NodeMetadata> rv = new ArrayList<NodeMetadata>();
    for (int i = 0; i < seeds; i++) {
      rv.add(nodes.get(i));
    }
    return rv;
  }

  private List<String> getPrivateIps(List<NodeMetadata> nodes) {
    return Lists.transform(Lists.newArrayList(nodes),
        new Function<NodeMetadata, String>() {
          @Override
          public String apply(NodeMetadata node) {
            return Iterables.get(node.getPrivateAddresses(), 0);
          }
        });
  }

  private Set<Instance> getInstances(List<NodeMetadata> nodes) {
    return Sets.newHashSet(Collections2.transform(Sets.newHashSet(nodes),
        new Function<NodeMetadata, Instance>() {
          @Override
          public Instance apply(NodeMetadata node) {
            try {
              return new Instance(node.getCredentials(), Collections
                  .singleton(CASSANDRA_ROLE), InetAddress.getByName(Iterables
                  .get(node.getPublicAddresses(), 0)), InetAddress
                  .getByName(Iterables.get(node.getPrivateAddresses(), 0)));
            } catch (UnknownHostException e) {
              throw new RuntimeException(e);
            }
          }
        }));
  }

}
