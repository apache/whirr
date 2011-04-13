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

import com.google.common.collect.Sets;

import java.util.ServiceLoader;
import java.util.Set;

/**
 * This class is used to create {@link ClusterController} instances.
 * <p>
 * <i>Implementation note.</i> {@link ClusterController} implementations are discovered
 * using a Service Provider
 * Interface (SPI), described in {@link ServiceLoader}.
 */
public class ClusterControllerFactory {

  private ServiceLoader<ClusterController> serviceLoader =
    ServiceLoader.load(ClusterController.class);

  /**
   * Create an instance of a {@link ClusterController} according to the given
   * name. If the name is <code>null</code> then the default {@link ClusterController}
   * is returned.
   */
  public ClusterController create(String serviceName) {
    if (serviceName == null) {
      return new ClusterController();
    }
    for (ClusterController controller : serviceLoader) {
      if (controller.getName().equals(serviceName)) {
        return controller;
      }
    }
    return null;
  }

  /**
   * Return a collection of available {@link ClusterController} names.
   * @return the available service names
   */
  public Set<String> availableServices() {
    Set<String> result = Sets.newLinkedHashSet();
    for (ClusterController s : serviceLoader) {
      result.add(s.getName());
    }
    return result;
  }
}
