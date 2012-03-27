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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.command.AbstractClusterCommand;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.domain.Statement;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static org.apache.whirr.RolePredicates.anyRoleIn;
import static org.jclouds.compute.predicates.NodePredicates.withIds;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

public class RunScriptCommand extends AbstractClusterCommand {

  private OptionSpec<String> rolesOption = parser
    .accepts("roles", "List of comma separated role names. " +
      "E.g. zookeeper,hadoop-namenode")
    .withRequiredArg()
    .ofType(String.class);

  private OptionSpec<String> instancesOption = parser
    .accepts("instances", "List of comma separated instance IDs")
    .withRequiredArg()
    .ofType(String.class);

  private OptionSpec<String> scriptOption = parser
    .accepts("script", "Path to script file to execute.")
    .withRequiredArg()
    .ofType(String.class);

  public RunScriptCommand() {
    this(new ClusterControllerFactory());
  }

  public RunScriptCommand(ClusterControllerFactory factory) {
    this(factory, new ClusterStateStoreFactory());
  }

  public RunScriptCommand(ClusterControllerFactory factory,
                          ClusterStateStoreFactory stateStoreFactory) {
    super("run-script", "Run a script on a specific instance or a " +
      "group of instances matching a role name", factory, stateStoreFactory);
  }

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
                 List<String> args) throws Exception {

    OptionSet optionSet = parser.parse(args.toArray(new String[0]));
    if (!optionSet.has(scriptOption)) {
      err.println("Please specify a script file to be executed.");
      err.println("Get more help: whirr help " + getName());
      return -1;
    }

    if (!(new File(optionSet.valueOf(scriptOption))).exists()) {
      err.printf("Script file '%s' not found.", optionSet.valueOf(scriptOption));
      err.println("Get more help: whirr help " + getName());
      return -2;
    }
    try {
      ClusterSpec clusterSpec = getClusterSpec(optionSet);
      String[] ids = null;
      String[] roles = null;
      if (optionSet.has(instancesOption)) {
        ids = optionSet.valueOf(instancesOption).split(",");
      }
      if (optionSet.has(rolesOption)) {
        roles = optionSet.valueOf(rolesOption).split(",");
      }
      printProviderInfo(out, err, clusterSpec, optionSet);
      return run(in, out, err, clusterSpec, ids, roles, optionSet.valueOf(scriptOption));

    } catch (IllegalArgumentException e) {
      printErrorAndHelpHint(err, e);
      return -3;
    }
  }

  public int run(InputStream in, PrintStream out, PrintStream err,
                 ClusterSpec clusterSpec, String[] instances, String[] roles,
                 String fileName) throws Exception {
    ClusterController controller = createClusterController(clusterSpec.getServiceName());
    Predicate<NodeMetadata> condition = buildFilterPredicate(instances, roles, clusterSpec);

    return handleScriptOutput(out, err, controller.runScriptOnNodesMatching(
      clusterSpec, condition, execFile(fileName)));
  }

  private Predicate<NodeMetadata> buildFilterPredicate(String[] ids, String[] roles, ClusterSpec spec)
    throws IOException {

    Predicate<NodeMetadata> condition = Predicates.alwaysTrue();
    if (ids != null && ids.length > 0) {
      return Predicates.and(condition, withIds(ids));
    } else if (roles != null && roles.length > 0) {
      List<String> instanceIds = Lists.newArrayList();

      Cluster cluster = createClusterStateStore(spec).load();
      for (Cluster.Instance instance : cluster.getInstancesMatching(
        anyRoleIn(Sets.<String>newHashSet(roles)))) {
        instanceIds.add(instance.getId());
      }

      condition = Predicates.and(condition,
        withIds(instanceIds.toArray(new String[0])));
      return condition;
    } else {
      return condition;
    }
  }


  private int handleScriptOutput(PrintStream out, PrintStream err,
                                 Map<? extends NodeMetadata, ExecResponse> responses) {
    int exitStatus = 0;
    for (Map.Entry<? extends NodeMetadata, ExecResponse> entry : responses.entrySet()) {
      out.printf("** Node %s: %s%n", entry.getKey().getId(),
        Iterables.concat(entry.getKey().getPrivateAddresses(),
          entry.getKey().getPublicAddresses()));

      ExecResponse response = entry.getValue();
      if (response.getExitCode() != 0) {
        exitStatus = response.getExitCode();
      }
      out.printf("%s%n", response.getOutput());
      err.printf("%s%n", response.getError());
    }
    return exitStatus;
  }

  private Statement execFile(String filePath) throws IOException {
    return exec(getFileContent(filePath));
  }

  private String getFileContent(String filePath) throws IOException {
    return StringUtils.join(Files.readLines(new File(filePath),
      Charset.defaultCharset()),
      "\n");
  }

  @Override
  public void printUsage(PrintStream stream) throws IOException {
    stream.println("Usage: whirr run-script [OPTIONS] --script <script> " +
      "[--instances id1,id2] [--roles role1,role2]");
    stream.println();
    parser.printHelpOn(stream);
  }

}
