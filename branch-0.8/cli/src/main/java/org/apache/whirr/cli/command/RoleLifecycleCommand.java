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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.command.AbstractClusterCommand;
import org.apache.whirr.state.ClusterStateStoreFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;

public abstract class RoleLifecycleCommand  extends AbstractClusterCommand {

  private final static ImmutableSet<String> EMPTYSET = ImmutableSet.of();

  private OptionSpec<String> rolesOption = parser
      .accepts("roles", "Cluster roles to target")
      .withRequiredArg().ofType(String.class);

  private OptionSpec<String> instanceIdsOption = parser
      .accepts("instance-ids", "Cluster instance IDs to target")
      .withRequiredArg().ofType(String.class);

  public RoleLifecycleCommand(String name, String description,
      ClusterControllerFactory factory, ClusterStateStoreFactory stateStoreFactory) {
    super(name, description, factory, stateStoreFactory);
  }

  /**
   * Implement this method to trigger the relevant role lifecycle action
   */
  public abstract int runLifecycleStep(ClusterSpec clusterSpec, ClusterController controller, OptionSet optionSet)
      throws IOException, InterruptedException;

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    OptionSet optionSet = parser.parse(args.toArray(new String[args.size()]));
    if (!optionSet.nonOptionArguments().isEmpty()) {
      printUsage(err);
      return -1;
    }

    try {
      ClusterSpec clusterSpec = getClusterSpec(optionSet);
      printProviderInfo(out, err, clusterSpec, optionSet);
      return runLifecycleStep(
          clusterSpec,
          createClusterController(clusterSpec.getServiceName()),
          optionSet
      );

    } catch (IllegalArgumentException e) {
      printErrorAndHelpHint(err, e);
      return -1;
    }
  }

  /**
   * Get the list of targeted roles for this command or an empty set
   */
  protected Set<String> getTargetRolesOrEmpty(OptionSet optionSet) {
    if (optionSet.hasArgument(rolesOption)) {
      return newHashSet(Splitter.on(",").split(optionSet.valueOf(rolesOption)));
    }
    return EMPTYSET;
  }

  /**
   * Get the list of targeted instance IDs for this command or an empty set
   */
  protected Set<String> getTargetInstanceIdsOrEmpty(OptionSet optionSet) {
    if (optionSet.hasArgument(instanceIdsOption)) {
      return newHashSet(Splitter.on(",").split(optionSet.valueOf(instanceIdsOption)));
    }
    return EMPTYSET;
  }
  
  /**
   * Print a generic usage indication for this class of commands
   */
  @Override
  public void printUsage(PrintStream stream) throws IOException {
    stream.println("Usage: whirr " + getName() + " [OPTIONS] [--roles role1,role2] [--instance-ids id1,id2]");
    stream.println();
    parser.printHelpOn(stream);
  }

}
