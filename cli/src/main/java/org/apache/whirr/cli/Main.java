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

package org.apache.whirr.cli;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.whirr.command.Command;
import org.apache.whirr.service.ClusterActionHandler;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.SortedSet;

/**
 * The entry point for the Whirr CLI.
 */
public class Main {
  
  private Map<String, Command> commandMap = Maps.newLinkedHashMap();
  private int maxLen = 0;
  
  Main(Command... commands) throws IOException {
    for (Command command : commands) {
      commandMap.put(command.getName(), command);
      maxLen = Math.max(maxLen, command.getName().length());
    }
  }
  
  int run(InputStream in, PrintStream out, PrintStream err,
      List<String> list) throws Exception {
    if (list.isEmpty()) {
      printUsage(out);
      return -1;
    }
    Command command = commandMap.get(list.get(0));
    if (command == null) {
      err.printf("Unrecognized command '%s'\n", list.get(0));
      err.println();
      printUsage(err);
      return -1;
    }
    return command.run(in, out, err, list.subList(1, list.size()));
  }
  
  private void printUsage(PrintStream stream) {
    stream.println("Usage: whirr COMMAND [ARGS]");
    stream.println("where COMMAND may be one of:");
    stream.println();
    for (Command command : commandMap.values()) {
      stream.printf("%" + maxLen + "s  %s\n", command.getName(),
          command.getDescription());
    }
    stream.println();
    stream.println("Available roles for instances:");
    for(String roleName : getSortedRoleNames()) {
      stream.println("  " + roleName);
    }
  }

  private static SortedSet<String> getSortedRoleNames() {
    ServiceLoader<ClusterActionHandler> loader =
     ServiceLoader.load(ClusterActionHandler.class);

    SortedSet<String> roles = Sets.newTreeSet();
    for(ClusterActionHandler handler : loader) {
      roles.add(handler.getRole());
    }
    return roles;
  }

  public static void main(String... args) throws Exception {
    ServiceLoader<Command> loader = ServiceLoader.load(Command.class);
    Main main = new Main(Lists.newArrayList(loader).toArray(new Command[0]));

    int rc = main.run(System.in, System.out, System.err, Arrays.asList(args));
    System.exit(rc);
  }
}
