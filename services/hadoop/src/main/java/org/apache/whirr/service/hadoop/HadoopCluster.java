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

package org.apache.whirr.service.hadoop;

import com.google.common.collect.Iterables;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.RolePredicates;

public class HadoopCluster {
  
  public static final int NAMENODE_PORT = 8020;
  public static final int NAMENODE_WEB_UI_PORT = 50070;
  public static final int JOBTRACKER_PORT = 8021;
  public static final int JOBTRACKER_WEB_UI_PORT = 50030;
  
  public static InetAddress getNamenodePublicAddress(Cluster cluster) throws IOException {
    return cluster.getInstanceMatching(
        RolePredicates.role(HadoopNameNodeClusterActionHandler.ROLE))
        .getPublicAddress();
  }
  public static InetAddress getNamenodePrivateAddress(Cluster cluster) throws IOException {
    return cluster.getInstanceMatching(
        RolePredicates.role(HadoopNameNodeClusterActionHandler.ROLE))
        .getPrivateAddress();
  }
  private static Instance getJobTracker(Cluster cluster) {
    Set<Instance> jobtracker = cluster.getInstancesMatching(
        RolePredicates.role(HadoopJobTrackerClusterActionHandler.ROLE));
    if (jobtracker.isEmpty()) {
      return null;
    }
    return Iterables.getOnlyElement(jobtracker);
  }
  public static InetAddress getJobTrackerPublicAddress(Cluster cluster) throws IOException {
    Instance jt = getJobTracker(cluster);
    return jt == null ? null : jt.getPublicAddress();
  }
  public static InetAddress getJobTrackerPrivateAddress(Cluster cluster) throws IOException {
    Instance jt = getJobTracker(cluster);
    return jt == null ? null : jt.getPrivateAddress();
  }
}
