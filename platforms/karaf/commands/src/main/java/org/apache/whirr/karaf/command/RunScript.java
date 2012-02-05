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

import org.apache.felix.gogo.commands.Argument;
import org.apache.felix.gogo.commands.Command;
import org.apache.felix.gogo.commands.Option;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.cli.command.RunScriptCommand;
import org.apache.whirr.karaf.command.support.WhirrCommandSupport;

import java.util.ArrayList;
import java.util.List;

@Command(scope = "whirr", name = "run-script", description = "Run a script on a specific instance or a group of instances matching a role name")
public class RunScript extends WhirrCommandSupport {

  @Option(required = false, name = "--instances", description = "The instance ids")
  protected List<String> instances = new ArrayList<String>();

  @Option(required = false, name = "--roles", description = "The roles")
  protected List<String> roles = new ArrayList<String>();

  @Argument(index = 0, name = "script", description = "The url of the sciprt to execute")
  protected String script;

  @Override
  protected Object doExecute() throws Exception {
    RunScriptCommand command = new RunScriptCommand(clusterControllerFactory);
    ClusterSpec clusterSpec = getClusterSpec();
    if (clusterSpec != null) {
      command.run(System.in, System.out, System.err, clusterSpec,
        instances.toArray(new String[instances.size()]), roles.toArray(new String[roles.size()]), script);
    }
    return null;
  }
}
