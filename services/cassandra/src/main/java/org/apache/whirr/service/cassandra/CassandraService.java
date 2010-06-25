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
import static org.jclouds.compute.options.TemplateOptions.Builder.runScript;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceBuilder;
import org.apache.whirr.service.RunUrlBuilder;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceSpec;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;

public class CassandraService extends Service {

  public static final String CASSANDRA_ROLE = "cassandra";
  public static final int CLIENT_PORT = 9160;

  public CassandraService(ServiceSpec serviceSpec) {
    super(serviceSpec);
  }

  @Override
  public Cluster launchCluster(ClusterSpec clusterSpec)
      throws IOException {

    ComputeService computeService = ComputeServiceBuilder.build(serviceSpec);

    byte[] bootScript = RunUrlBuilder.runUrls("sun/java/install",
        "apache/cassandra/install");
    Template template = computeService.templateBuilder().osFamily(
        OsFamily.UBUNTU).options(
        runScript(bootScript).installPrivateKey(serviceSpec.readPrivateKey())
            .authorizePublicKey(serviceSpec.readPublicKey()).inboundPorts(22,
                CLIENT_PORT)).build();

    InstanceTemplate instanceTemplate = clusterSpec
        .getInstanceTemplate(CASSANDRA_ROLE);
    checkNotNull(instanceTemplate);
    int clusterSize = instanceTemplate.getNumberOfInstances();
    Set<? extends NodeMetadata> nodeMap;
    try {
      nodeMap = computeService.runNodesWithTag(serviceSpec.getClusterName(),
          clusterSize, template);
    } catch (RunNodesException e) {
      // TODO: can we do better here
      throw new IOException(e);
    }
    List<NodeMetadata> nodes = Lists.newArrayList(nodeMap);
    List<NodeMetadata> seeds = getSeeds(nodes);

    // Pass list of all servers in cluster to configure script.
    String servers = Joiner.on(' ').join(getPrivateIps(seeds));
    byte[] configureScript = RunUrlBuilder
        .runUrls("apache/cassandra/post-configure " + servers);
    try {
      computeService.runScriptOnNodesWithTag(serviceSpec.getClusterName(),
          configureScript);
    } catch (RunScriptOnNodesException e) {
      // TODO: retry
      throw new IOException(e);
    }

    return new Cluster(getInstances(nodes));
  }

  /**
   * Pick a selection of the nodes that are to become seeds.
   * TODO improve selection method. Right now it picks 20% of the nodes
   * as seeds, or a minimum of one node if it is a small cluster. 
   * @param nodes all nodes in cluster
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
            return Iterables.get(node.getPrivateAddresses(), 0)
                .getHostAddress();
          }
        });
  }

  private Set<Instance> getInstances(List<NodeMetadata> nodes) {
    return Sets.newHashSet(Collections2.transform(Sets.newHashSet(nodes),
        new Function<NodeMetadata, Instance>() {
          @Override
          public Instance apply(NodeMetadata node) {
            return new Instance(Collections.singleton(CASSANDRA_ROLE),
                Iterables.get(node.getPublicAddresses(), 0), Iterables.get(node
                    .getPrivateAddresses(), 0));
          }
        }));
  }

}
