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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.ServiceLoader;

import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ClusterActionHandlerFactory;

import com.google.common.base.Function;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;

/**
 * HandlerMapFactory used in ScriptBasedClusterAction classes to create a map of action handlers.
 * This implementation allow reuse of the same handler map by multiple ScriptBasedClusterAction
 * instances.
 * 
 * Note: Another motivation of extracting this code outside of ScriptBasedClusterAction is that the
 * same code inside action implementation is not possible to mock by Mockito for JUnit testing
 * purposes. Without this code the action classes can be plugged with another factory and remain
 * JUnit testable.
 */
public class HandlerMapFactory {

   protected static final class ReturnHandlerByRoleOrPrefix implements Function<String, ClusterActionHandler> {
      private final Map<String, ClusterActionHandlerFactory> factoryMap;
      private final Map<String, ClusterActionHandler> handlerMap;

      protected ReturnHandlerByRoleOrPrefix(Map<String, ClusterActionHandlerFactory> factoryMap,
               Map<String, ClusterActionHandler> handlerMap) {
         this.factoryMap = checkNotNull(factoryMap, "factoryMap");
         this.handlerMap = checkNotNull(handlerMap, "handlerMap");
      }

      @Override
      public ClusterActionHandler apply(String arg0) {
         checkNotNull(arg0, "role");
         for (String prefix : factoryMap.keySet()) {
            if (arg0.startsWith(prefix)) {
               return checkNotNull(
                 factoryMap.get(prefix).create(arg0.substring(prefix.length())),
                 "Unable to create the action handler"
               );
            }
         }
         return checkNotNull(handlerMap.get(arg0), "Action handler not found");
      }
   }

   public Map<String, ClusterActionHandler> create() {
      return create(ServiceLoader.load(ClusterActionHandlerFactory.class),
        ServiceLoader.load(ClusterActionHandler.class));
   }

   public Map<String, ClusterActionHandler> create(
      Iterable<ClusterActionHandlerFactory> factories,
      Iterable<ClusterActionHandler> handlers
   ) {
      Map<String, ClusterActionHandlerFactory> factoryMap =
          indexFactoriesByRolePrefix(checkNotNull(factories, "factories"));
      Map<String, ClusterActionHandler> handlerMap =
          indexHandlersByRole(checkNotNull(handlers, "handlers"));

      return new MapMaker().makeComputingMap(
        new ReturnHandlerByRoleOrPrefix(factoryMap, handlerMap));
   }

   static Map<String, ClusterActionHandlerFactory> indexFactoriesByRolePrefix(
            Iterable<ClusterActionHandlerFactory> handlers) {
      return Maps.uniqueIndex(handlers, new Function<ClusterActionHandlerFactory, String>() {

         @Override
         public String apply(ClusterActionHandlerFactory arg0) {
            return arg0.getRolePrefix();
         }

      });
   }

   static Map<String, ClusterActionHandler> indexHandlersByRole(Iterable<ClusterActionHandler> handlers) {
      return Maps.uniqueIndex(handlers, new Function<ClusterActionHandler, String>() {

         @Override
         public String apply(ClusterActionHandler arg0) {
            return arg0.getRole();
         }

      });
   }
}
