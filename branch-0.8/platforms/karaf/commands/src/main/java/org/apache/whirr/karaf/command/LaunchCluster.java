/*
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

package org.apache.whirr.karaf.command;

import org.apache.felix.gogo.commands.Command;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.cli.command.LaunchClusterCommand;
import org.apache.whirr.karaf.command.support.WhirrCommandSupport;

@Command(scope = "whirr", name = "launch-cluster", description = "Launch a new cluster running a service.")
public class LaunchCluster extends WhirrCommandSupport {

  @Override
  protected Object doExecute() throws Exception {
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    LaunchClusterCommand command = new LaunchClusterCommand(clusterControllerFactory);
    ClusterSpec clusterSpec = getClusterSpec();
    if (clusterSpec != null) {
      command.run(System.in, System.out, System.err, clusterSpec);
    }
    return null;
  }
}
