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
import org.apache.whirr.command.AbstractClusterCommand;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

/**
 * A command to destroy a running cluster (terminate and cleanup).
 */
public class DestroyClusterCommand extends AbstractClusterCommand {

  public DestroyClusterCommand() throws IOException {
    this(new ClusterControllerFactory());
  }

  public DestroyClusterCommand(ClusterControllerFactory factory) {
    super("destroy-cluster", "Terminate and cleanup resources for a running cluster.", factory);
  }

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
                 List<String> args) throws Exception {

    OptionSet optionSet = parser.parse(args.toArray(new String[args.size()]));

    if (!optionSet.nonOptionArguments().isEmpty()) {
      printUsage(err);
      return -1;
    }
    try {
      ClusterSpec clusterSpec = getClusterSpec(optionSet);
      return run(in, out, err, clusterSpec);

    } catch (IllegalArgumentException e) {
      printErrorAndHelpHint(err, e);
      return -1;
    }
  }

  public int run(InputStream in, PrintStream out, PrintStream err,
                 ClusterSpec clusterSpec) throws Exception {
    ClusterController controller = createClusterController(clusterSpec.getServiceName());
    controller.destroyCluster(clusterSpec);
    return 0;
  }

}
