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

import java.util.ServiceLoader;
import java.util.Set;

import com.google.common.collect.Sets;

/**
 * This class is used to create {@link Service} instances.
 * <p>
 * <i>Implementation note.</i> {@link Service} implementations are discovered
 * using a Service Provider
 * Interface (SPI), described in {@link ServiceLoader}.
 */
public class ServiceFactory {

  private ServiceLoader<Service> serviceLoader =
    ServiceLoader.load(Service.class);

  /**
   * Create an instance of a {@link Service} according to the given
   * name. If the name is <code>null</code> then the default {@link Service}
   * is returned.
   */
  public Service create(String serviceName) {
    if (serviceName == null) {
      return new Service();
    }
    for (Service service : serviceLoader) {
      if (service.getName().equals(serviceName)) {
        return service;
      }
    }
    return null;
  }

  /**
   * Return a collection of available services.
   * @return the available service names
   */
  public Set<String> availableServices() {
    Set<String> result = Sets.newLinkedHashSet();
    for (Service s : serviceLoader) {
      result.add(s.getName());
    }
    return result;
  }
}
