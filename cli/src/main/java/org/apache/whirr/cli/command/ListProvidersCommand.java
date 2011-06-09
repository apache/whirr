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

package org.apache.whirr.cli.command;

import com.google.common.collect.ImmutableSet;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.command.AbstractClusterCommand;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Set;

public class ListProvidersCommand extends AbstractClusterCommand {

  Set<String> testedComputeProviders = ImmutableSet.of("aws-ec2",
      "cloudservers-us", "cloudservers-uk", "byon");

  public ListProvidersCommand() {
    super("list-providers", "Show a list of the supported providers",
        new ClusterControllerFactory());
  }

  @Override
  public int run(InputStream in, PrintStream out,
                 PrintStream err, List<String> args) throws Exception {

    if (args.size() == 0) {
      printUsage(out);

    } else {
      String type = args.get(0);

      if ("compute".equals(type)) {
        listComputeProviders(out);
      } else if ("blobstore".equals(type)) {
        listBlobstoreProviders(out);
      }
    }

    return 0;
  }

  private void printUsage(PrintStream out) {
    out.println("whirr list-providers <compute OR blobstore>");
  }

  private void listBlobstoreProviders(PrintStream out) {
    for(ProviderMetadata blobstore : Providers.allBlobStore()) {
      out.println("* " + blobstore.getName());

      out.println("\tHomepage: " + blobstore.getHomepage());
      out.println("\tConsole: " + blobstore.getConsole());
      out.println("\tAPI: " + blobstore.getApiDocumentation());

      out.println("\tConfiguration options:");

      out.println("\t\twhirr.blobstore-provider = " + blobstore.getId());
      out.println("\t\twhirr.blobstore-identity = <" + blobstore.getIdentityName() +">");
      out.println("\t\twhirr.blobstore-credential = <" + blobstore.getCredentialName() + ">\n");
    }
  }

  private void listComputeProviders(PrintStream out) {
    for(ProviderMetadata provider : Providers.allCompute()) {
      if (testedComputeProviders.contains(provider.getId())) {
        out.println("* " + provider.getName() + " - tested");
      } else {
        out.println("* " + provider.getName());
      }

      out.println("\tHomepage: " + provider.getHomepage());
      out.println("\tConsole: " + provider.getConsole());
      out.println("\tAPI: " + provider.getApiDocumentation());

      out.println("\tConfiguration options:");

      out.println("\t\twhirr.provider = " + provider.getId());
      out.println("\t\twhirr.identity =  <" + provider.getIdentityName() + ">");
      out.println("\t\twhirr.credential = <" + provider.getCredentialName() + ">\n");
    }
  }
}
