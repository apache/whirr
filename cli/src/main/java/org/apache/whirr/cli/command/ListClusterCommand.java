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

import com.google.common.base.Joiner;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.command.AbstractClusterCommand;
import org.apache.whirr.service.ClusterStateStore;
import org.apache.whirr.service.ClusterStateStoreFactory;

/**
 * A command to list the nodes in a cluster.
 */
public class ListClusterCommand extends AbstractClusterCommand {

  public ListClusterCommand() throws IOException {
    this(new ClusterControllerFactory());
  }

  public ListClusterCommand(ClusterControllerFactory factory) {
    this(factory, new ClusterStateStoreFactory());
  }

  public ListClusterCommand(ClusterControllerFactory factory,
                            ClusterStateStoreFactory stateStoreFactory) {
    super("list-cluster", "List the nodes in a cluster.", factory, stateStoreFactory);
  }
  
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err,
      List<String> args) throws Exception {
    
    OptionSet optionSet = parser.parse(args.toArray(new String[0]));

    if (!optionSet.nonOptionArguments().isEmpty()) {
      printUsage(parser, err);
      return -1;
    }
    try {
      ClusterSpec clusterSpec = getClusterSpec(optionSet);
      ClusterStateStore stateStore = createClusterStateStore(clusterSpec);
      ClusterController controller = createClusterController(clusterSpec.getServiceName());

      for (Cluster.Instance instance : controller.getInstances(clusterSpec, stateStore)) {
        out.println(Joiner.on('\t').join(
            instance.getId(),
            instance.getNodeMetadata().getImageId(),
            instance.getPublicIp(),
            instance.getPrivateIp(),
            instance.getNodeMetadata().getState(),
            instance.getNodeMetadata().getLocation().getId(),
            Joiner.on(",").join(instance.getRoles())
          )
        );
      }
      return 0;
    } catch (IllegalArgumentException e) {
      err.println(e.getMessage());
      printUsage(parser, err);
      return -1;
    }
  }

  private void printUsage(OptionParser parser, PrintStream stream) throws IOException {
    stream.println("Usage: whirr list-cluster [OPTIONS]");
    stream.println();
    parser.printHelpOn(stream);
  }
}
