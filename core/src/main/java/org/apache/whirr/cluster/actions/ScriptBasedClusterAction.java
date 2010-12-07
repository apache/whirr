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

package org.apache.whirr.cluster.actions;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.Map;
import java.util.ServiceLoader;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterAction;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.service.jclouds.StatementBuilder;
import org.jclouds.scriptbuilder.domain.Statements;

/**
 * A {@link ClusterAction} that provides the base functionality for running
 * scripts on instances in the cluster.
 */
public abstract class ScriptBasedClusterAction extends ClusterAction {

  private ServiceLoader<ClusterActionHandler> clusterActionHandlerLoader =
    ServiceLoader.load(ClusterActionHandler.class);

  protected abstract void doAction(Map<InstanceTemplate, ClusterActionEvent> eventMap)
      throws IOException, InterruptedException;

  public Cluster execute(ClusterSpec clusterSpec, Cluster cluster) throws IOException, InterruptedException {
    
    Map<String, ClusterActionHandler> handlerMap = Maps.newHashMap();
    for (ClusterActionHandler handler : clusterActionHandlerLoader) {
      handlerMap.put(handler.getRole(), handler);
    }

    Map<InstanceTemplate, ClusterActionEvent> eventMap = Maps.newHashMap();
    Cluster newCluster = cluster;
    for (InstanceTemplate instanceTemplate : clusterSpec.getInstanceTemplates()) {
      StatementBuilder statementBuilder = new StatementBuilder();
      statementBuilder.addStatement(Statements.call("installRunUrl"));
      ClusterActionEvent event = new ClusterActionEvent(getAction(),
          clusterSpec, newCluster, statementBuilder);
      eventMap.put(instanceTemplate, event);
      for (String role : instanceTemplate.getRoles()) {
        ClusterActionHandler handler = handlerMap.get(role);
        if (handler == null) {
          throw new IllegalArgumentException("No handler for role " + role);
        }
        handler.beforeAction(event);
      }
      newCluster = event.getCluster(); // cluster may have been updated by handler 
    }
    
    doAction(eventMap);
    
    // cluster may have been updated by action
    newCluster = Iterables.get(eventMap.values(), 0).getCluster();

    for (InstanceTemplate instanceTemplate : clusterSpec.getInstanceTemplates()) {
      for (String role : instanceTemplate.getRoles()) {
        ClusterActionHandler handler = handlerMap.get(role);
        if (handler == null) {
          throw new IllegalArgumentException("No handler for role " + role);
        }
        ClusterActionEvent event = eventMap.get(instanceTemplate);
        event.setCluster(newCluster);
        handler.afterAction(event);
        newCluster = event.getCluster(); // cluster may have been updated by handler 
      }
    }
    
    return newCluster;
  }

}
