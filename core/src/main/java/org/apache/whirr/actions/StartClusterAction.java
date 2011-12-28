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

package org.apache.whirr.actions;

import com.google.common.base.Function;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionHandler;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A {@link ClusterAction} for starting the cluster services
 */
public class StartClusterAction extends ScriptBasedClusterAction {

  private static final Logger LOG = LoggerFactory.getLogger(StartClusterAction.class);

  public StartClusterAction(
    final Function<ClusterSpec, ComputeServiceContext> getCompute,
    final Map<String, ClusterActionHandler> handlerMap) {
    super(getCompute, handlerMap);
  }

  @Override
  protected String getAction() {
    return ClusterActionHandler.START_ACTION;
  }
}
