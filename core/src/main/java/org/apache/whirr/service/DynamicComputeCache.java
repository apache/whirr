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
import com.google.common.base.Objects;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * A class for adding {@link ComputeServiceContext} on runtime.
 */
public class DynamicComputeCache implements Function<ClusterSpec, ComputeServiceContext> {
   
  private static final Logger LOG = LoggerFactory.getLogger(DynamicComputeCache.class);

  private final Map<Key, ComputeServiceContext> serviceContextMap = new HashMap<Key, ComputeServiceContext>();

  @Override
  public ComputeServiceContext apply(ClusterSpec arg0) {
    return serviceContextMap.get(new Key(arg0));
  }

  public void bind(ComputeService computeService) {
    if (computeService != null) {
      serviceContextMap.put(new Key(computeService), computeService.getContext());
    }
  }

  public void unbind(ComputeService computeService) {
    if (computeService != null) {
      serviceContextMap.remove(new Key(computeService));
    }
  }

  /**
   * Key class for the compute context cache
   */
  private static class Key {
    private String provider;
    private final String key;

    public Key(ClusterSpec spec) {
      provider = spec.getProvider();

      key = Objects.toStringHelper("").omitNullValues()
              .add("provider", provider).toString();
    }

    public Key(ComputeService computeService) {
      provider = computeService.getContext().unwrap().getId();

      key = Objects.toStringHelper("").omitNullValues()
              .add("provider", provider).toString();
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof Key) {
        return Objects.equal(this.key, ((Key)that).key);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
              .add("provider", provider)
              .toString();
    }
  }
}
