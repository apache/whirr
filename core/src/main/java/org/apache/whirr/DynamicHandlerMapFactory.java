/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr;

import static org.apache.whirr.util.Utils.convertMapToLoadingCache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.whirr.service.ClusterActionHandler;

import com.google.common.cache.LoadingCache;

public class DynamicHandlerMapFactory extends HandlerMapFactory {

  protected final Map<String, ClusterActionHandler> clusterActionHandlerMap = new ConcurrentHashMap<String, ClusterActionHandler>();
  protected final LoadingCache<String, ClusterActionHandler> cache = convertMapToLoadingCache(clusterActionHandlerMap);

  @Override
  public LoadingCache<String, ClusterActionHandler> create() {
    return cache;
  }

  public void bind(ClusterActionHandler clusterActionHandler) {
    if (clusterActionHandler != null && clusterActionHandler.getRole() != null) {
      clusterActionHandlerMap.put(clusterActionHandler.getRole(), clusterActionHandler);
      cache.invalidate(clusterActionHandler.getRole());
    }
  }

  public void unbind(ClusterActionHandler clusterActionHandler) {
    if (clusterActionHandler != null && clusterActionHandler.getRole() != null) {
      cache.invalidate(clusterActionHandler.getRole());
      clusterActionHandlerMap.remove(clusterActionHandler.getRole());
    }
  }
}
