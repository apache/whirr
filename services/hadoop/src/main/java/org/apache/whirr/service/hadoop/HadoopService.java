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
import static org.apache.whirr.service.RunUrlBuilder.runUrls;
import static org.jclouds.compute.options.TemplateOptions.Builder.runScript;
import static org.jclouds.io.Payloads.newStringPayload;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunScriptOnNodesException;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopService extends Service {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(HadoopService.class);
  
  public static final Set<String> MASTER_ROLE = Sets.newHashSet("nn", "jt");
  public static final Set<String> WORKER_ROLE = Sets.newHashSet("dn", "tt");
  
  public static final int WEB_PORT = 80;
  public static final int NAMENODE_PORT = 8020;
  public static final int JOBTRACKER_PORT = 8021;
  public static final int NAMENODE_WEB_UI_PORT = 50070;
  public static final int JOBTRACKER_WEB_UI_PORT = 50030;

  @Override
  public String getName() {
    return "hadoop";
  }
  
  @Override
  public HadoopCluster launchCluster(final ClusterSpec clusterSpec)
      throws IOException, InterruptedException {
    LOG.info("Launching " + clusterSpec.getClusterName() + " cluster");
    
    ExecutorService executorService = Executors.newCachedThreadPool();
    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(clusterSpec);
    final ComputeService computeService =
      computeServiceContext.getComputeService();
    
    String hadoopInstallRunUrl = clusterSpec.getConfiguration().getString(
      "whirr.hadoop-install-runurl", "apache/hadoop/install");
    
    Payload installScript = newStringPayload(runUrls(clusterSpec.getRunUrlBase(),
      "sun/java/install",
      String.format("%s -c %s", hadoopInstallRunUrl,
          clusterSpec.getProvider())));

    LOG.info("Configuring template");
    TemplateBuilder templateBuilder = computeService.templateBuilder()
      .options(runScript(installScript)
      .installPrivateKey(clusterSpec.getPrivateKey())
      .authorizePublicKey(clusterSpec.getPublicKey()));
    
    TemplateBuilderStrategy strategy = new HadoopTemplateBuilderStrategy();
    strategy.configureTemplateBuilder(clusterSpec, templateBuilder);
    
    final Template template = templateBuilder.build();

    InstanceTemplate masterInstanceTemplate =
        clusterSpec.getInstanceTemplate(MASTER_ROLE);
    checkNotNull(masterInstanceTemplate);
    checkArgument(masterInstanceTemplate.getNumberOfInstances() == 1);
    Future<Set<? extends NodeMetadata>> nodesFuture = executorService.submit(
        new Callable<Set<? extends NodeMetadata>>(){
      public Set<? extends NodeMetadata> call() throws Exception {
        LOG.info("Starting master node");
        Set<? extends NodeMetadata> nodes = computeService.runNodesWithTag(
          clusterSpec.getClusterName(), 1, template);
        LOG.info("Master node started: {}", nodes);
        return nodes;
      }
    });
    
    final InstanceTemplate workerInstanceTemplate =
        clusterSpec.getInstanceTemplate(WORKER_ROLE);
    checkNotNull(workerInstanceTemplate);
    Future<Set<? extends NodeMetadata>> workerNodesFuture =
      executorService.submit(new Callable<Set<? extends NodeMetadata>>(){
      public Set<? extends NodeMetadata> call() throws Exception {
        int num = workerInstanceTemplate.getNumberOfInstances();
        LOG.info("Starting {} worker node(s)", num);
        Set<? extends NodeMetadata> nodes =  computeService.runNodesWithTag(
            clusterSpec.getClusterName(), num, template);
        LOG.info("Worker nodes started: {}", nodes);
        return nodes;
      }
    });
    
    Set<? extends NodeMetadata> nodes;
    try {
      LOG.info("Waiting for master node to start...");
      nodes = nodesFuture.get();
    } catch (ExecutionException e) {
      // Check for RunNodesException
      // TODO: can we do better here (retry?)
      throw new IOException(e);
    }
    NodeMetadata node = Iterables.getOnlyElement(nodes);
    InetAddress namenodePublicAddress = InetAddress.getByName(Iterables.get(node.getPublicAddresses(),0));
    InetAddress jobtrackerPublicAddress = InetAddress.getByName(Iterables.get(node.getPublicAddresses(),0));
    
    LOG.info("Authorizing firewall");
    FirewallSettings.authorizeIngress(computeServiceContext, node, clusterSpec,
        WEB_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, node, clusterSpec,
        NAMENODE_WEB_UI_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, node, clusterSpec,
        JOBTRACKER_WEB_UI_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, node, clusterSpec,
        namenodePublicAddress.getHostAddress(), NAMENODE_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, node, clusterSpec,
        namenodePublicAddress.getHostAddress(), JOBTRACKER_PORT);
    if (!namenodePublicAddress.equals(jobtrackerPublicAddress)) {
      FirewallSettings.authorizeIngress(computeServiceContext, node, clusterSpec,
          jobtrackerPublicAddress.getHostAddress(), NAMENODE_PORT);
      FirewallSettings.authorizeIngress(computeServiceContext, node, clusterSpec,
          jobtrackerPublicAddress.getHostAddress(), JOBTRACKER_PORT);
    }

    String hadoopConfigureRunUrl = clusterSpec.getConfiguration().getString(
        "whirr.hadoop-configure-runurl", "apache/hadoop/post-configure");
    Payload nnjtConfigureScript = newStringPayload(runUrls(
        clusterSpec.getRunUrlBase(),
        String.format(
          "%s nn,jt -n %s -j %s -c %s",
          hadoopConfigureRunUrl,
          DnsUtil.resolveAddress(namenodePublicAddress.getHostAddress()),
          DnsUtil.resolveAddress(jobtrackerPublicAddress.getHostAddress()),
          clusterSpec.getProvider())));
    try {
      LOG.info("Running configure script on master");
      computeService.runScriptOnNodesMatching(withIds(node.getId()),
          nnjtConfigureScript);
    } catch (RunScriptOnNodesException e) {
      // TODO: retry
      throw new IOException(e);
    }
              
    Set<? extends NodeMetadata> workerNodes;
    try {
      workerNodes = workerNodesFuture.get();
    } catch (ExecutionException e) {
      // TODO: Check for RunNodesException and don't bail out if only a few have
      // failed to start
      throw new IOException(e);
    }

    Payload dnttConfigureScript = newStringPayload(runUrls(
        clusterSpec.getRunUrlBase(),
        String.format(
          "%s dn,tt -n %s -j %s -c %s",
          hadoopConfigureRunUrl,
          DnsUtil.resolveAddress(namenodePublicAddress.getHostAddress()),
          DnsUtil.resolveAddress(jobtrackerPublicAddress.getHostAddress()),
          clusterSpec.getProvider())));
    try {
      LOG.info("Running configure script on workers");
      Predicate<NodeMetadata> workerPredicate = Predicates.and(
          runningWithTag(clusterSpec.getClusterName()),
          Predicates.not(withIds(node.getId())));
      computeService.runScriptOnNodesMatching(workerPredicate,
          dnttConfigureScript);
    } catch (RunScriptOnNodesException e) {
      // TODO: retry
      throw new IOException(e);
    }
        
    // TODO: wait for TTs to come up (done in test for the moment)
    
    Set<Instance> instances = Sets.union(getInstances(MASTER_ROLE, Collections.singleton(node)),
      getInstances(WORKER_ROLE, workerNodes));
    
    LOG.info("Completed launch of {}", clusterSpec.getClusterName());
    LOG.info("Web UI available at http://{}",
      DnsUtil.resolveAddress(namenodePublicAddress.getHostAddress()));
    Properties config = createClientSideProperties(namenodePublicAddress, jobtrackerPublicAddress);
    createClientSideHadoopSiteFile(clusterSpec, config);
    HadoopCluster cluster = new HadoopCluster(instances, config);
    createProxyScript(clusterSpec, cluster);
    return cluster; 
  }
  
  private Set<Instance> getInstances(final Set<String> roles, Set<? extends NodeMetadata> nodes) {
    return Sets.newHashSet(Collections2.transform(Sets.newHashSet(nodes),
        new Function<NodeMetadata, Instance>() {
      @Override
      public Instance apply(NodeMetadata node) {
        try {
        return new Instance(node.getCredentials(), roles,
            InetAddress.getByName(Iterables.get(node.getPublicAddresses(), 0)),
            InetAddress.getByName(Iterables.get(node.getPrivateAddresses(), 0)));
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
      }
    }));
  }
  
  private static Predicate<NodeMetadata> withIds(String... ids) {
    checkNotNull(ids, "ids must be defined");
    final Set<String> search = Sets.newHashSet(ids);
    return new Predicate<NodeMetadata>() {
       @Override
       public boolean apply(NodeMetadata nodeMetadata) {
          return search.contains(nodeMetadata.getId());
       }
       @Override
       public String toString() {
          return "withIds(" + search + ")";
       }
    };
  }
  
  private Properties createClientSideProperties(InetAddress namenode, InetAddress jobtracker) throws IOException {
      Properties config = new Properties();
      config.setProperty("hadoop.job.ugi", "root,root");
      config.setProperty("fs.default.name", String.format("hdfs://%s:8020/", DnsUtil.resolveAddress(namenode.getHostAddress())));
      config.setProperty("mapred.job.tracker", String.format("%s:8021", DnsUtil.resolveAddress(jobtracker.getHostAddress())));
      config.setProperty("hadoop.socks.server", "localhost:6666");
      config.setProperty("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.SocksSocketFactory");
      return config;
  }

  private void createClientSideHadoopSiteFile(ClusterSpec clusterSpec, Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopSiteFile = new File(configDir, "hadoop-site.xml");
    try {
      Files.write(generateHadoopConfigurationFile(config), hadoopSiteFile,
          Charsets.UTF_8);
      LOG.info("Wrote Hadoop site file {}", hadoopSiteFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop site file {}", hadoopSiteFile, e);
    }
  }
  
  private File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }
  
  private CharSequence generateHadoopConfigurationFile(Properties config) {
    StringBuilder sb = new StringBuilder();
    sb.append("<?xml version=\"1.0\"?>\n");
    sb.append("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n");
    sb.append("<configuration>\n");
    for (Entry<Object, Object> entry : config.entrySet()) {
      sb.append("  <property>\n");
      sb.append("    <name>").append(entry.getKey()).append("</name>\n");
      sb.append("    <value>").append(entry.getValue()).append("</value>\n");
      sb.append("  </property>\n");
    }
    sb.append("</configuration>\n");
    return sb;
  }
  
  private void createProxyScript(ClusterSpec clusterSpec, HadoopCluster cluster) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopProxyFile = new File(configDir, "hadoop-proxy.sh");
    try {
      HadoopProxy proxy = new HadoopProxy(clusterSpec, cluster);
      String script = String.format("echo 'Running proxy to Hadoop cluster at %s. " +
          "Use Ctrl-c to quit.'\n",
          DnsUtil.resolveAddress(cluster.getNamenodePublicAddress().getHostAddress()))
          + Joiner.on(" ").join(proxy.getProxyCommand());
      Files.write(script, hadoopProxyFile, Charsets.UTF_8);
      LOG.info("Wrote Hadoop proxy script {}", hadoopProxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop proxy script {}", hadoopProxyFile, e);
    }
  }
  
}
