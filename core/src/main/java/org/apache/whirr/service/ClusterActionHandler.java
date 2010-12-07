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

import java.io.IOException;

/**
 * A callback interface for cluster actions that apply to instances in a
 * given role.
 * <p>
 * <i>Implementation note.</i> {@link ClusterActionHandler} implementations are
 * discovered using a Service Provider Interface (SPI), described in
 * {@link java.util.ServiceLoader}.
 */
public abstract class ClusterActionHandler {
  
  public static final String BOOTSTRAP_ACTION = "bootstrap";
  public static final String CONFIGURE_ACTION = "configure";
  public static final String DESTROY_ACTION = "destroy";

  public abstract String getRole();
  
  /**
   * Called before the action is performed, giving the implementation an
   * opportunity to specify scripts that should be run as a part of this
   * action.
   * @param event
   */
  public void beforeAction(ClusterActionEvent event)
      throws IOException, InterruptedException {
  }
  
  /**
   * Called after the action has been performed.
   * @param event
   */
  public void afterAction(ClusterActionEvent event)
      throws IOException, InterruptedException {
  }
}
