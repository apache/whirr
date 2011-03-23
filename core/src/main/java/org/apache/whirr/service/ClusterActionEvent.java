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

import org.apache.whirr.service.jclouds.StatementBuilder;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;

/**
 * An event object which is fired when a {@link ClusterAction} occurs. 
 */
public class ClusterActionEvent {
  
  private String action;
  private ClusterSpec clusterSpec;
  private Cluster cluster;
  private StatementBuilder statementBuilder;
  private TemplateBuilderStrategy templateBuilderStrategy =
    new TemplateBuilderStrategy();
  
  public ClusterActionEvent(String action, ClusterSpec clusterSpec,
      Cluster cluster) {
    this(action, clusterSpec, cluster, null);
  }
  
  public ClusterActionEvent(String action, ClusterSpec clusterSpec,
      Cluster cluster, StatementBuilder statementBuilder) {
    this.action = action;
    this.clusterSpec = clusterSpec;
    this.cluster = cluster;
    this.statementBuilder = statementBuilder;
  }
  
  public Cluster getCluster() {
    return cluster;
  }

  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  public String getAction() {
    return action;
  }

  public ClusterSpec getClusterSpec() {
    return clusterSpec;
  }

  public StatementBuilder getStatementBuilder() {
    return statementBuilder;
  }

  public TemplateBuilderStrategy getTemplateBuilderStrategy() {
    return templateBuilderStrategy;
  }

  public void setTemplateBuilderStrategy(
      TemplateBuilderStrategy templateBuilderStrategy) {
    this.templateBuilderStrategy = templateBuilderStrategy;
  }

}
