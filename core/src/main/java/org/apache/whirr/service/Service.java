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

import static org.jclouds.compute.predicates.NodePredicates.withTag;

import com.google.common.base.Predicate;

import java.io.IOException;
import java.util.Set;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;

/**
 * This class represents a service that a client wants to use. This class is
 * used to start and stop clusters that provide the service.
 */
public abstract class Service {

  /**
   * @return the unique name of the service.
   */
  public abstract String getName();

  /**
   * Start the cluster described by <code>clusterSpec</code> and block until the
   * cluster is
   * available. It is not guaranteed that the service running on the cluster
   * has started when this method returns.
   * @param clusterSpec
   * @return an object representing the running cluster
   * @throws IOException if there is a problem while starting the cluster. The
   * cluster may or may not have started.
   */
  public abstract Cluster launchCluster(ClusterSpec clusterSpec)
    throws IOException;
  
  /**
   * Stop the cluster and destroy all resources associated with it.
   * @throws IOException if there is a problem while stopping the cluster. The
   * cluster may or may not have been stopped.
   */
  public void destroyCluster(ClusterSpec clusterSpec) throws IOException {
    ComputeService computeService =
      ComputeServiceContextBuilder.build(clusterSpec).getComputeService();
    computeService.destroyNodesMatching(withTag(clusterSpec.getClusterName()));
  }
  
  public Set<? extends NodeMetadata> getNodes(ClusterSpec clusterSpec)
    throws IOException {
    ComputeService computeService =
      ComputeServiceContextBuilder.build(clusterSpec).getComputeService();
    return computeService.listNodesDetailsMatching(
        runningWithTag(clusterSpec.getClusterName()));
  }
  
  public static Predicate<ComputeMetadata> runningWithTag(final String tag) {
    return new Predicate<ComputeMetadata>() {
      @Override
      public boolean apply(ComputeMetadata computeMetadata) {
        // Not all list calls return NodeMetadata (e.g. VCloud)
        if (computeMetadata instanceof NodeMetadata) {
          NodeMetadata nodeMetadata = (NodeMetadata) computeMetadata;
          return tag.equals(nodeMetadata.getTag())
            && nodeMetadata.getState() == NodeState.RUNNING;
        }
        return false;
      }
      @Override
      public String toString() {
        return "runningWithTag(" + tag + ")";
      }
    };
  }

}
