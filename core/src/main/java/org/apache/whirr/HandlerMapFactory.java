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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.ServiceLoader;
import java.util.Set;

import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ClusterActionHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
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
  private static final Logger LOG =
    LoggerFactory.getLogger(HandlerMapFactory.class);

  protected static final class ReturnHandlerByRoleOrPrefix extends CacheLoader<String, ClusterActionHandler> {
    private final Map<String, ClusterActionHandlerFactory> factoryMap;
    private final Map<String, ClusterActionHandler> handlerMap;

    protected ReturnHandlerByRoleOrPrefix(Map<String, ClusterActionHandlerFactory> factoryMap,
        Map<String, ClusterActionHandler> handlerMap) {
      this.factoryMap = checkNotNull(factoryMap, "factoryMap");
      this.handlerMap = checkNotNull(handlerMap, "handlerMap");
    }

    @Override
    public ClusterActionHandler load(final String role) {
      checkNotNull(role, "role");
      Set<String> prefixes = factoryMap.keySet();
      try {
        String prefix = Iterables.find(prefixes, new Predicate<String>() {

          @Override
          public boolean apply(String prefix) {
            return role.startsWith(prefix);
          }

        });
        LOG.debug("role {} starts with a configured prefix {}", role, prefix);
        ClusterActionHandlerFactory factory = factoryMap.get(prefix);
        checkArgument(factory != null, "could not create action handler factory %s", prefix);
        String subrole = role.substring(prefix.length());
        ClusterActionHandler returnVal = factory.create(subrole);
        checkArgument(returnVal != null, "action handler factory %s could not create action handler for role %s",
            prefix, subrole);
        return returnVal;
      } catch (NoSuchElementException e) {
        LOG.debug("role {} didn't start with any of the configured prefixes {}", role, prefixes);
        ClusterActionHandler returnVal = handlerMap.get(role);
        checkArgument(returnVal != null, "Action handler not found for role: %s; configured roles %s", role,
            handlerMap.keySet());
        return returnVal;
      }

    }
  }

  public LoadingCache<String, ClusterActionHandler> create() {
    return create(ServiceLoader.load(ClusterActionHandlerFactory.class),
      ServiceLoader.load(ClusterActionHandler.class));
  }

  public LoadingCache<String, ClusterActionHandler> create(
    Iterable<ClusterActionHandlerFactory> factories,
    Iterable<ClusterActionHandler> handlers
  ) {
    Map<String, ClusterActionHandlerFactory> factoryMap =
       indexFactoriesByRolePrefix(checkNotNull(factories, "factories"));
    Map<String, ClusterActionHandler> handlerMap =
       indexHandlersByRole(checkNotNull(handlers, "handlers"));
    return CacheBuilder.newBuilder().build(new ReturnHandlerByRoleOrPrefix(factoryMap, handlerMap));
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
