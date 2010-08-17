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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.apache.whirr.cli.Command;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ServiceFactory;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;

/**
 * An abstract command for interacting with clusters.
 */
public abstract class ClusterSpecCommand extends Command {

  protected ServiceFactory factory;
  
  protected OptionParser parser = new OptionParser();
  protected OptionSpec<String> cloudProvider = parser.accepts("cloud-provider")
      .withRequiredArg().defaultsTo("ec2").ofType(String.class);
  protected OptionSpec<String> cloudIdentity = parser.accepts("cloud-identity")
      .withRequiredArg().ofType(String.class);
  protected OptionSpec<String> cloudCredential = parser.accepts("cloud-credential")
      .withRequiredArg().ofType(String.class);
  protected OptionSpec<String> secretKeyFile = parser.accepts("secret-key-file")
      .withRequiredArg()
      .defaultsTo(System.getProperty("user.home") + "/.ssh/id_rsa")
      .ofType(String.class);
  
  public ClusterSpecCommand(String name, String description, ServiceFactory factory) {
    super(name, description);
    this.factory = factory;
  }
  
  protected ClusterSpec getClusterSpec(OptionSet optionSet,
        InstanceTemplate... instanceTemplates) {
    return getClusterSpec(optionSet, Arrays.asList(instanceTemplates));
  }
  
  protected ClusterSpec getClusterSpec(OptionSet optionSet,
        List<InstanceTemplate> instanceTemplates) {
    ClusterSpec clusterSpec = new ClusterSpec(instanceTemplates);
    clusterSpec.setProvider(optionSet.valueOf(cloudProvider));
    clusterSpec.setIdentity(checkNotNull(optionSet.valueOf(cloudIdentity),
        "cloud-identity"));
    clusterSpec.setCredential(optionSet.valueOf(cloudCredential));
    clusterSpec.setSecretKeyFile(optionSet.valueOf(secretKeyFile));
    return clusterSpec;
  }

}
