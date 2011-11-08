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

package org.apache.whirr.util;

import java.io.PrintStream;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;

public class WhirrUtils {

  /**
   * Prints ssh commands that can be used to login into the nodes
   * 
   * @param out
   * @param clusterSpec
   * @param cluster
   */
  public static void printAccess(PrintStream out, ClusterSpec clusterSpec,
      Cluster cluster, int maxPrint) {
    out.println("You can log into instances using the following ssh commands:");

    String user = clusterSpec.getLoginUser() != null ? clusterSpec
        .getLoginUser() : clusterSpec.getClusterUser();

    String pkFile = clusterSpec.getPrivateKeyFile().getAbsolutePath();

    int counter = 0;
    for (Instance instance : cluster.getInstances()) {

      out.printf(
          "'ssh -i %s -o \"UserKnownHostsFile /dev/null\" -o StrictHostKeyChecking=no %s@%s'\n",
          pkFile, user, instance.getPublicIp());

      if (counter > maxPrint) {
        out.println("... Too many instances, truncating.");
        break;
      }
      counter++;
    }
  }
}
