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

package org.apache.whirr;

import static org.apache.whirr.service.ClusterActionHandler.BOOTSTRAP_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.CONFIGURE_ACTION;
import static org.apache.whirr.service.ClusterActionHandler.DESTROY_ACTION;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.whirr.actions.ByonClusterAction;
import org.apache.whirr.service.ClusterActionHandler;
import org.jclouds.compute.domain.NodeMetadata;

/**
 * Equivalent of {@link ClusterController}, but for execution in BYON mode
 * ("bring your own nodes").
 */
public class ByonClusterController extends ClusterController {
  
  @Override
  public String getName() {
    return "byon";
  }

  public Cluster launchCluster(ClusterSpec clusterSpec) throws IOException,
      InterruptedException {

    Map<String, ClusterActionHandler> handlerMap = new HandlerMapFactory()
        .create();

    ClusterAction bootstrapper = new ByonClusterAction(BOOTSTRAP_ACTION, getCompute(), handlerMap);
    Cluster cluster = bootstrapper.execute(clusterSpec, null);

    ClusterAction configurer = new ByonClusterAction(CONFIGURE_ACTION, getCompute(), handlerMap);
    cluster = configurer.execute(clusterSpec, cluster);

    return cluster;
  }

  public void destroyCluster(ClusterSpec clusterSpec) throws IOException,
      InterruptedException {
    Map<String, ClusterActionHandler> handlerMap = new HandlerMapFactory()
        .create();

    ClusterAction destroyer = new ByonClusterAction(DESTROY_ACTION, getCompute(), handlerMap);
    destroyer.execute(clusterSpec, null);
  }
  
  @Override
  public void destroyInstance(ClusterSpec clusterSpec, String instanceId)
      throws IOException {
    // TODO
  }
  
  @Override
  public Set<? extends NodeMetadata> getNodes(ClusterSpec clusterSpec)
      throws IOException, InterruptedException {
    // TODO return singleton with trivial NodeMetadata for localhost?
    return null;
  }
}
