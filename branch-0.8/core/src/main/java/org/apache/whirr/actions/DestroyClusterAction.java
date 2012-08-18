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

import static org.jclouds.compute.predicates.NodePredicates.inGroup;

import java.io.IOException;
import java.util.Map;

import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.cache.LoadingCache;

/**
 * A {@link ClusterAction} for tearing down a running cluster and freeing up all
 * its resources.
 */
public class DestroyClusterAction extends ScriptBasedClusterAction {

  private static final Logger LOG = LoggerFactory
      .getLogger(DestroyClusterAction.class);

  public DestroyClusterAction(
      final Function<ClusterSpec, ComputeServiceContext> getCompute,
      final LoadingCache<String, ClusterActionHandler> handlerMap) {
    super(getCompute, handlerMap);
  }

  @Override
  protected String getAction() {
    return ClusterActionHandler.DESTROY_ACTION;
  }

  @Override
  protected void postRunScriptsActions(
      Map<InstanceTemplate, ClusterActionEvent> eventMap) throws IOException {
    ClusterSpec clusterSpec = eventMap.values().iterator().next()
        .getClusterSpec();
    LOG.info("Destroying " + clusterSpec.getClusterName() + " cluster");
    ComputeService computeService = getCompute().apply(clusterSpec)
        .getComputeService();
    computeService.destroyNodesMatching(inGroup(clusterSpec.getClusterName()));
    LOG.info("Cluster {} destroyed", clusterSpec.getClusterName());
  }

}
