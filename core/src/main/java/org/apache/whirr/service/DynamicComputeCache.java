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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;

import java.util.HashMap;
import java.util.Map;

/**
 * A class for adding {@link ComputeServiceContext} on runtime.
 */
public class DynamicComputeCache implements Function<ClusterSpec, ComputeServiceContext> {

  private final Map<String, ComputeServiceContext> serviceContextMap = new HashMap<String, ComputeServiceContext>();

  @Override
  public ComputeServiceContext apply(ClusterSpec arg0) {
    String name = arg0.getContextName();
    if (!Strings.isNullOrEmpty(name)) {
      return serviceContextMap.get(name);
    } else {
      return ComputeCache.INSTANCE.apply(arg0);
    }
  }

  public synchronized void bind(ComputeService computeService) {
    if (computeService != null) {
      serviceContextMap.put(computeService.getContext().unwrap().getName(), computeService.getContext());
    }
  }

  public synchronized void unbind(ComputeService computeService) {
    if (computeService != null) {
      serviceContextMap.remove(computeService.getContext().unwrap().getName());
    }
  }
}
