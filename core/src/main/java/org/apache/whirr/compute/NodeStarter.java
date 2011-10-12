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

package org.apache.whirr.compute;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.Callable;

public class NodeStarter implements Callable<Set<NodeMetadata>> {

  private static final Logger LOG =
    LoggerFactory.getLogger(NodeStarter.class);

  final private ComputeService computeService;
  final private String clusterName;
  final private Set<String> roles;
  final private int num;
  final private Template template;

  public NodeStarter(final ComputeService computeService, final String clusterName,
    final Set<String> roles, final int num, final Template template) {
    this.computeService = computeService;
    this.clusterName = clusterName;
    this.roles = roles;
    this.num = num;
    this.template = template;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Set<NodeMetadata> call() throws Exception {
    LOG.info("Starting {} node(s) with roles {}", num,
        roles);
    Set<NodeMetadata> nodes = (Set<NodeMetadata>)computeService
      .createNodesInGroup(clusterName, num, template);
    LOG.info("Nodes started: {}", nodes);
    return nodes;
  }
}
