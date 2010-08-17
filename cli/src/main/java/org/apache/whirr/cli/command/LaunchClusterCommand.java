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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceFactory;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;

import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.inject.internal.Lists;

/**
 * A command to launch a new cluster.
 */
public class LaunchClusterCommand extends ClusterSpecCommand {

  public LaunchClusterCommand() throws IOException {
    this(new ServiceFactory());
  }

  public LaunchClusterCommand(ServiceFactory factory) {
    super("launch-cluster", "Launch a new cluster running a service.", factory);
  }

  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    
    OptionSet optionSet = parser.parse(args.toArray(new String[0]));

    List<String> nonOptionArguments = optionSet.nonOptionArguments();
    if (nonOptionArguments.size() < 4) {
      printUsage(parser, err);
      return -1;
    }
    
    final int nonTemplateArgumentsSize = 2;

    if ((nonOptionArguments.size() - nonTemplateArgumentsSize) % 2 == 1) {
      printUsage(parser, err);
      return -1;
    }

    List<InstanceTemplate> templates = Lists.newArrayList();
    for (int i = 0; i < (nonOptionArguments.size() - 1) / 2; i++) {
      int number = Integer.parseInt(nonOptionArguments.get(2 * i + nonTemplateArgumentsSize));
      String rolesString = nonOptionArguments.get(2 * i + nonTemplateArgumentsSize + 1);
      Set<String> roles = Sets.newHashSet(Splitter.on("+").split(rolesString));
      templates.add(new InstanceTemplate(number, roles));
    }
    ClusterSpec clusterSpec = getClusterSpec(optionSet, templates);
    String serviceName = nonOptionArguments.get(0);
    clusterSpec.setClusterName(nonOptionArguments.get(1));
    
    Service service = factory.create(serviceName);
    Cluster cluster = service.launchCluster(clusterSpec);
    out.printf("Started cluster of %s instances\n",
        cluster.getInstances().size());
    out.println(cluster);
    return 0;
  }

  private void printUsage(OptionParser parser, PrintStream stream) throws IOException {
    stream.println("Usage: whirr launch-cluster [OPTIONS] <service-name> " +
        "<cluster-name> <num> <roles> [<num> <roles>]*");
    stream.println();
    parser.printHelpOn(stream);
  }
}
