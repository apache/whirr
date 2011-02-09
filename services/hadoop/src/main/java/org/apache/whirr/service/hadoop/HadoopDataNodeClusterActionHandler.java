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

import static org.apache.whirr.service.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.whirr.net.DnsUtil;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.ClusterSpec;

public class HadoopDataNodeClusterActionHandler extends ClusterActionHandlerSupport {

  public static final String ROLE = "hadoop-datanode";
  
  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();   
    addStatement(event, call("configure_hostnames", "-c", clusterSpec.getProvider()));
    String hadoopInstallFunction = clusterSpec.getConfiguration().getString(
        "whirr.hadoop-install-function", "install_hadoop");
    addStatement(event, call("install_java"));
    addStatement(event, call(hadoopInstallFunction, "-c", clusterSpec.getProvider()));
    event.setTemplateBuilderStrategy(new HadoopTemplateBuilderStrategy());
  }
  
  @Override
  protected void beforeConfigure(ClusterActionEvent event)
      throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    Instance instance = cluster.getInstanceMatching(
        role(HadoopNameNodeClusterActionHandler.ROLE));
    InetAddress namenodePublicAddress = instance.getPublicAddress();
    InetAddress jobtrackerPublicAddress = namenodePublicAddress;

    String hadoopConfigureFunction = clusterSpec.getConfiguration().getString(
        "whirr.hadoop-configure-function", "configure_hadoop");
    addStatement(event, call(hadoopConfigureFunction,
        "hadoop-datanode,hadoop-tasktracker",
        "-n", DnsUtil.resolveAddress(namenodePublicAddress.getHostAddress()),
        "-j", DnsUtil.resolveAddress(jobtrackerPublicAddress.getHostAddress()),
        "-c", clusterSpec.getProvider()
    ));
  }
  
}
