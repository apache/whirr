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

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.Set;

import org.apache.whirr.cluster.actions.BootstrapClusterAction;
import org.apache.whirr.cluster.actions.ConfigureClusterAction;
import org.apache.whirr.cluster.actions.DestroyClusterAction;
import org.apache.whirr.net.DnsUtil;
import org.apache.whirr.service.Cluster.Instance;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents a service that a client wants to use. This class is
 * used to start and stop clusters that provide the service.
 */
public class Service {

  private static final Logger LOG = LoggerFactory.getLogger(Service.class);

  /**
   * @return the unique name of the service.
   */
  public String getName() {
    throw new UnsupportedOperationException("No service name");
  }
  /**
   * Start the cluster described by <code>clusterSpec</code> and block until the
   * cluster is
   * available. It is not guaranteed that the service running on the cluster
   * has started when this method returns.
   * @param clusterSpec
   * @return an object representing the running cluster
   * @throws IOException if there is a problem while starting the cluster. The
   * cluster may or may not have started.
   * @throws InterruptedException if the thread is interrupted.
   */
  public Cluster launchCluster(ClusterSpec clusterSpec)
      throws IOException, InterruptedException {
    
    BootstrapClusterAction bootstrapper = new BootstrapClusterAction();
    Cluster cluster = bootstrapper.execute(clusterSpec, null);

    ConfigureClusterAction configurer = new ConfigureClusterAction();
    cluster = configurer.execute(clusterSpec, cluster);
    
    createInstancesFile(clusterSpec, cluster);
    
    return cluster;
  }
  
  private void createInstancesFile(ClusterSpec clusterSpec, Cluster cluster)
      throws IOException {
    
    File clusterDir = clusterSpec.getClusterDirectory();
    File instancesFile = new File(clusterDir, "instances");
    StringBuilder sb = new StringBuilder();
    for (Instance instance : cluster.getInstances()) {
      String id = instance.getId();
      String roles = Joiner.on(',').join(instance.getRoles());
      String publicAddress = DnsUtil.resolveAddress(instance.getPublicAddress()
          .getHostAddress());
      String privateAddress = instance.getPrivateAddress().getHostAddress();
      sb.append(id).append("\t");
      sb.append(roles).append("\t");
      sb.append(publicAddress).append("\t");
      sb.append(privateAddress).append("\n");
    }
    try {
      Files.write(sb.toString(), instancesFile, Charsets.UTF_8);
      LOG.info("Wrote instances file {}", instancesFile);
    } catch (IOException e) {
      LOG.error("Problem writing instances file {}", instancesFile, e);
    }
  }
  
  /**
   * Stop the cluster and destroy all resources associated with it.
   * @throws IOException if there is a problem while stopping the cluster. The
   * cluster may or may not have been stopped.
   * @throws InterruptedException if the thread is interrupted.
   */
  public void destroyCluster(ClusterSpec clusterSpec) throws IOException,
      InterruptedException {
    DestroyClusterAction destroyer = new DestroyClusterAction();
    destroyer.execute(clusterSpec, null);
    Files.deleteRecursively(clusterSpec.getClusterDirectory());
  }
  
  public Set<? extends NodeMetadata> getNodes(ClusterSpec clusterSpec)
    throws IOException, InterruptedException {
    ComputeService computeService =
      ComputeServiceContextBuilder.build(clusterSpec).getComputeService();
    return computeService.listNodesDetailsMatching(
        runningInGroup(clusterSpec.getClusterName()));
  }
  
  public static Predicate<ComputeMetadata> runningInGroup(final String group) {
    return new Predicate<ComputeMetadata>() {
      @Override
      public boolean apply(ComputeMetadata computeMetadata) {
        // Not all list calls return NodeMetadata (e.g. VCloud)
        if (computeMetadata instanceof NodeMetadata) {
          NodeMetadata nodeMetadata = (NodeMetadata) computeMetadata;
          return group.equals(nodeMetadata.getGroup())
            && nodeMetadata.getState() == NodeState.RUNNING;
        }
        return false;
      }
      @Override
      public String toString() {
        return "runningInGroup(" + group + ")";
      }
    };
  }

}
