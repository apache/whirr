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

package org.apache.whirr.service.jclouds;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterSpec;
import org.jclouds.aws.util.AWSUtils;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.ec2.EC2Client;
import org.jclouds.ec2.domain.IpProtocol;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Utility functions for controlling firewall settings for a cluster.
 */
public class FirewallSettings {
  
  /**
   * @return the IP address of the client on which this code is running.
   * @throws IOException
   */
  public static String getOriginatingIp() throws IOException {
    URL url = new URL("http://checkip.amazonaws.com/");
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    connection.connect();
    return IOUtils.toString(connection.getInputStream()).trim() + "/32";
  }

  public static void authorizeIngress(ComputeServiceContext computeServiceContext,
      Instance instance, ClusterSpec clusterSpec, int... ports) throws IOException {
    
    authorizeIngress(computeServiceContext, Collections.singleton(instance),
        clusterSpec, ports);
  }
  
  public static void authorizeIngress(ComputeServiceContext computeServiceContext,
      Instance instance, ClusterSpec clusterSpec, String ip, int... ports) {
    
    authorizeIngress(computeServiceContext, Collections.singleton(instance),
        clusterSpec, Lists.newArrayList(ip + "/32"), ports);
  }

  public static void authorizeIngress(ComputeServiceContext computeServiceContext,
      Set<Instance> instances, ClusterSpec clusterSpec, int... ports) throws IOException {
    List<String> cidrs = clusterSpec.getClientCidrs();
    if (cidrs == null || cidrs.isEmpty()) {
      cidrs = Lists.newArrayList(getOriginatingIp());
    }
    authorizeIngress(computeServiceContext, instances, clusterSpec, cidrs, ports);
  }

  private static void authorizeIngress(ComputeServiceContext computeServiceContext,
      Set<Instance> instances, ClusterSpec clusterSpec, List<String> cidrs, int... ports) {
    
    if (computeServiceContext.getProviderSpecificContext().getApi() instanceof
          EC2Client) {
      // This code (or something like it) may be added to jclouds (see
      // http://code.google.com/p/jclouds/issues/detail?id=336).
      // Until then we need this temporary workaround.
      String region = AWSUtils.parseHandle(Iterables.get(instances, 0).getId())[0];
      EC2Client ec2Client = EC2Client.class.cast(
          computeServiceContext.getProviderSpecificContext().getApi());
      String groupName = "jclouds#" + clusterSpec.getClusterName() + "#" + region;
      for (String cidr : cidrs) {
        for (int port : ports) {
          ec2Client.getSecurityGroupServices()
            .authorizeSecurityGroupIngressInRegion(region, groupName,
                IpProtocol.TCP, port, port, cidr);
        }
      }
    }
  }

}
