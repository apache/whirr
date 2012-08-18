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

import org.apache.whirr.command.Command;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.ServiceLoader;

public class HelpCommand extends Command {

  public HelpCommand() {
    super("help", "Show help about an action");
  }

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    if (args.size() == 0) {
      printUsage(out);
      return -1;
    }

    String helpForCommand = args.get(0);
    ServiceLoader<Command> loader = ServiceLoader.load(Command.class);
    for(Command command : loader) {
      if (command.getName().equals(helpForCommand)) {
        command.printUsage(out);
        return 0;
      }
    }

    err.println("No command found with that name: " + helpForCommand);
    return -2;
  }

  @Override
  public void printUsage(PrintStream stream) throws IOException {
    stream.println("Usage: whirr help <command>");
  }
}
