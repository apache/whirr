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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.jclouds.compute.options.TemplateOptions.Builder.runScript;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceBuilder;
import org.apache.whirr.service.RunUrlBuilder;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceSpec;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;

public class HadoopService extends Service {
  
  public static final Set<String> MASTER_ROLE = Sets.newHashSet("nn", "jt");
  public static final Set<String> WORKER_ROLE = Sets.newHashSet("dn", "tt");

  public HadoopService(ServiceSpec serviceSpec) {
    super(serviceSpec);
  }

  @Override
  public HadoopCluster launchCluster(ClusterSpec clusterSpec) throws IOException {
    ComputeService computeService = ComputeServiceBuilder.build(serviceSpec);

    String privateKey = serviceSpec.readPrivateKey();
    String publicKey = serviceSpec.readPublicKey();
    
    // deal with user packages and autoshutdown with extra runurls
    byte[] nnjtBootScript = RunUrlBuilder.runUrls(
      "sun/java/install",
      String.format("apache/hadoop/install nn,jt -c %s", serviceSpec.getProvider()));
    
    Template template = computeService.templateBuilder()
    .osFamily(OsFamily.UBUNTU)
    .options(runScript(nnjtBootScript)
        .installPrivateKey(privateKey)
	      .authorizePublicKey(publicKey)
	      .inboundPorts(22, 80, 8020, 8021, 50030)) // TODO: restrict further
    .build();
    
    InstanceTemplate instanceTemplate = clusterSpec.getInstanceTemplate(MASTER_ROLE);
    checkNotNull(instanceTemplate);
    checkArgument(instanceTemplate.getNumberOfInstances() == 1);
    Set<? extends NodeMetadata> nodes;
    try {
      nodes = computeService.runNodesWithTag(
      serviceSpec.getClusterName(), 1, template);
    } catch (RunNodesException e) {
      // TODO: can we do better here (retry?)
      throw new IOException(e);
    }
    NodeMetadata node = Iterables.getOnlyElement(nodes);
    InetAddress namenodePublicAddress = Iterables.getOnlyElement(node.getPublicAddresses());
    InetAddress jobtrackerPublicAddress = Iterables.getOnlyElement(node.getPublicAddresses());
    
    byte[] slaveBootScript = RunUrlBuilder.runUrls(
      "sun/java/install",
      String.format("apache/hadoop/install dn,tt -n %s -j %s",
	      namenodePublicAddress.getHostName(),
	      jobtrackerPublicAddress.getHostName()));

    template = computeService.templateBuilder()
    .osFamily(OsFamily.UBUNTU)
    .options(runScript(slaveBootScript)
        .installPrivateKey(privateKey)
	      .authorizePublicKey(publicKey))
    .build();

    instanceTemplate = clusterSpec.getInstanceTemplate(WORKER_ROLE);
    checkNotNull(instanceTemplate);

    Set<? extends NodeMetadata> workerNodes;
    try {
      workerNodes = computeService.runNodesWithTag(serviceSpec.getClusterName(),
	instanceTemplate.getNumberOfInstances(), template);
    } catch (RunNodesException e) {
      // TODO: don't bail out if only a few have failed to start
      throw new IOException(e);
    }
    
    // TODO: wait for TTs to come up (done in test for the moment)
    
    Set<Instance> instances = Sets.union(getInstances(MASTER_ROLE, Collections.singleton(node)),
	getInstances(WORKER_ROLE, workerNodes));
    
    Properties config = createClientSideProperties(namenodePublicAddress, jobtrackerPublicAddress);
    return new HadoopCluster(instances, config);
  }
  
  private Set<Instance> getInstances(final Set<String> roles, Set<? extends NodeMetadata> nodes) {
    return Sets.newHashSet(Collections2.transform(Sets.newHashSet(nodes),
	new Function<NodeMetadata, Instance>() {
      @Override
      public Instance apply(NodeMetadata node) {
	return new Instance(roles,
	    Iterables.get(node.getPublicAddresses(), 0),
	    Iterables.get(node.getPrivateAddresses(), 0));
      }
    }));
  }
  
  private Properties createClientSideProperties(InetAddress namenode, InetAddress jobtracker) throws IOException {
      Properties config = new Properties();
      config.setProperty("hadoop.job.ugi", "root,root");
      config.setProperty("fs.default.name", String.format("hdfs://%s:8020/", namenode.getHostName()));
      config.setProperty("mapred.job.tracker", String.format("%s:8021", jobtracker.getHostName()));
      config.setProperty("hadoop.socks.server", "localhost:6666");
      config.setProperty("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.SocksSocketFactory");
      return config;
  }

  private void createClientSideHadoopSiteFile(InetAddress namenode, InetAddress jobtracker) throws IOException {
    File file = new File("/tmp/hadoop-site.xml");
    Files.write(generateHadoopConfigurationFile(createClientSideProperties(namenode, jobtracker)), file, Charsets.UTF_8);
  }
  
  private CharSequence generateHadoopConfigurationFile(Properties config) {
    StringBuilder sb = new StringBuilder();
    sb.append("<?xml version=\"1.0\"?>\n");
    sb.append("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n");
    sb.append("<configuration>\n");
    for (Entry<Object, Object> entry : config.entrySet()) {
      sb.append("<property>\n");
      sb.append("<name>").append(entry.getKey()).append("</name>\n");
      sb.append("<value>").append(entry.getValue()).append("</value>\n");
      sb.append("</property>\n");
    }
    sb.append("</configuration>\n");
    return sb;
  }
  
}
