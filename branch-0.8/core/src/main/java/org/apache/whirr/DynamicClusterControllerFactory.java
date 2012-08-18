/*
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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;

/**
 * A Dynamic Controller Factory.
 * This Factory can be used inside dynamic environments and leverage their dynamic nature.
 */
public class DynamicClusterControllerFactory extends ClusterControllerFactory {

  private static final String BASIC_CONTROLLER = "basic";
  private final Map<String, ClusterController> clusterControllerMap = new ConcurrentHashMap<String, ClusterController>();

   /**
   * Create an instance of a {@link ClusterController} according to the given
   * name. If the name is <code>null</code> then the default {@link ClusterController}
   * is returned.
   */
  @Override
  public ClusterController create(String serviceName) {
    if (serviceName == null) {
      return clusterControllerMap.get(BASIC_CONTROLLER);
    } else return clusterControllerMap.get(serviceName);
  }

  /**
   * Return a collection of available {@link ClusterController} names.
   * @return the available service names
   */
  @Override
  public Set<String> availableServices() {
    Set<String> result = Sets.newLinkedHashSet();
    for (Map.Entry<String,ClusterController> entry: clusterControllerMap.entrySet()) {
      result.add(entry.getKey());
    }
    return result;
  }

  public void bind(ClusterController clusterController) {
    if (clusterController != null) {
      String name = null;
      try{
        name = clusterController.getName();
      } catch (UnsupportedOperationException ex) {
        name = BASIC_CONTROLLER;
      }
      clusterControllerMap.put(name, clusterController);
    }
  }

  public void unbind(ClusterController clusterController) {
    if (clusterController != null)  {
      try {
       clusterControllerMap.remove(clusterController.getName());
      }catch(UnsupportedOperationException ex) {
        //Ignore
      }
    }
  }
}
