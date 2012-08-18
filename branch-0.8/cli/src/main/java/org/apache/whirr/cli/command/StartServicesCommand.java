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

package org.apache.whirr.cli.command;

import joptsimple.OptionSet;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.state.ClusterStateStoreFactory;

import java.io.IOException;

/**
 * A command to start the cluster services
 */
public class StartServicesCommand extends RoleLifecycleCommand {

  public StartServicesCommand() throws IOException {
    this(new ClusterControllerFactory());
  }

  public StartServicesCommand(ClusterControllerFactory factory) {
    this(factory, new ClusterStateStoreFactory());
  }

  public StartServicesCommand(ClusterControllerFactory factory,
                              ClusterStateStoreFactory stateStoreFactory) {
    super("start-services", "Start the cluster services.", factory, stateStoreFactory);
  }

  @Override
  public int runLifecycleStep(ClusterSpec clusterSpec, ClusterController controller, OptionSet optionSet)
      throws IOException, InterruptedException {
    controller.startServices(
        clusterSpec,
        getCluster(clusterSpec, controller),
        getTargetRolesOrEmpty(optionSet),
        getTargetInstanceIdsOrEmpty(optionSet)
    );
    return 0;
  }
}
