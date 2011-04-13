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

import com.google.common.collect.Maps;

import java.util.Map;
import java.util.ServiceLoader;

import org.apache.whirr.service.ClusterActionHandler;

/**
 * HandlerMapFactory used in ScriptBasedClusterAction classes to 
 * create a map of action handlers.
 * This implementation allow reuse of the same handler map by 
 * multiple ScriptBasedClusterAction instances.
 * 
 * Note: Another motivation of extracting this code outside of ScriptBasedClusterAction
 * is that the same code inside action implementation is not possible to mock by Mockito
 * for JUnit testing purposes. Without this code the action classes can be plugged with
 * another factory and remain JUnit testable.
 */
class HandlerMapFactory {

  private ServiceLoader<ClusterActionHandler> clusterActionHandlerLoader =
    ServiceLoader.load(ClusterActionHandler.class);
  
  Map<String, ClusterActionHandler> create() {
    Map<String, ClusterActionHandler> handlerMap = Maps.newHashMap();
    for (ClusterActionHandler handler : clusterActionHandlerLoader) {
      handlerMap.put(handler.getRole(), handler);
    }
    return handlerMap;
  }
}
