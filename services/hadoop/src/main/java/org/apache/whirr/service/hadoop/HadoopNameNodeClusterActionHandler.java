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
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildCommon;
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildHdfs;
import static org.apache.whirr.service.hadoop.HadoopConfigurationBuilder.buildMapReduce;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.net.DnsUtil;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ComputeServiceContextBuilder;
import org.apache.whirr.service.jclouds.FirewallSettings;
import org.jclouds.compute.ComputeServiceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopNameNodeClusterActionHandler extends ClusterActionHandlerSupport {

  private static final Logger LOG =
    LoggerFactory.getLogger(HadoopNameNodeClusterActionHandler.class);
  
  public static final String ROLE = "hadoop-namenode";
  
  public static final int NAMENODE_PORT = 8020;
  public static final int JOBTRACKER_PORT = 8021;
  public static final int NAMENODE_WEB_UI_PORT = 50070;
  public static final int JOBTRACKER_WEB_UI_PORT = 50030;
    
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
    addStatement(event, call("install_tarball"));
    addStatement(event, call(hadoopInstallFunction, "-c", clusterSpec.getProvider()));
  }
  
  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    LOG.info("Authorizing firewall");
    Instance instance = cluster.getInstanceMatching(role(ROLE));
    InetAddress namenodePublicAddress = instance.getPublicAddress();
    InetAddress jobtrackerPublicAddress = namenodePublicAddress;
    
    ComputeServiceContext computeServiceContext =
      ComputeServiceContextBuilder.build(clusterSpec);
    FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
        NAMENODE_WEB_UI_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
        JOBTRACKER_WEB_UI_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
        namenodePublicAddress.getHostAddress(), NAMENODE_PORT);
    FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
        namenodePublicAddress.getHostAddress(), JOBTRACKER_PORT);
    if (!namenodePublicAddress.equals(jobtrackerPublicAddress)) {
      FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
          jobtrackerPublicAddress.getHostAddress(), NAMENODE_PORT);
      FirewallSettings.authorizeIngress(computeServiceContext, instance, clusterSpec,
          jobtrackerPublicAddress.getHostAddress(), JOBTRACKER_PORT);
    }
    
    try {
      event.getStatementBuilder().addStatements(
        buildCommon("/tmp/core-site.xml", clusterSpec, cluster),
        buildHdfs("/tmp/hdfs-site.xml", clusterSpec, cluster),
        buildMapReduce("/tmp/mapred-site.xml", clusterSpec, cluster)
      );
    } catch (ConfigurationException e) {
      throw new IOException(e);
    }
    
    String hadoopConfigureFunction = clusterSpec.getConfiguration().getString(
        "whirr.hadoop-configure-function", "configure_hadoop");
    addStatement(event, call(hadoopConfigureFunction,
        "hadoop-namenode,hadoop-jobtracker",
        "-c", clusterSpec.getProvider()
    ));
  }
  
  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    
    // TODO: wait for TTs to come up (done in test for the moment)
    
    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    Instance instance = cluster.getInstanceMatching(role(ROLE));
    InetAddress namenodePublicAddress = instance.getPublicAddress();
    InetAddress jobtrackerPublicAddress = namenodePublicAddress;

    LOG.info("Namenode web UI available at http://{}:{}",
      DnsUtil.resolveAddress(namenodePublicAddress.getHostAddress()),
      NAMENODE_WEB_UI_PORT);
    LOG.info("Jobtracker web UI available at http://{}:{}",
      DnsUtil.resolveAddress(jobtrackerPublicAddress.getHostAddress()),
      JOBTRACKER_WEB_UI_PORT);
    Properties config = createClientSideProperties(clusterSpec, namenodePublicAddress, jobtrackerPublicAddress);
    createClientSideHadoopSiteFile(clusterSpec, config);
    createProxyScript(clusterSpec, cluster);
    event.setCluster(new Cluster(cluster.getInstances(), config));
  }

  private Properties createClientSideProperties(ClusterSpec clusterSpec,
      InetAddress namenode, InetAddress jobtracker) throws IOException {
    Properties config = new Properties();
    config.setProperty("hadoop.job.ugi", "root,root");
    config.setProperty("fs.default.name", String.format("hdfs://%s:8020/", DnsUtil.resolveAddress(namenode.getHostAddress())));
    config.setProperty("mapred.job.tracker", String.format("%s:8021", DnsUtil.resolveAddress(jobtracker.getHostAddress())));
    config.setProperty("hadoop.socks.server", "localhost:6666");
    config.setProperty("hadoop.rpc.socket.factory.class.default", "org.apache.hadoop.net.SocksSocketFactory");
    if (clusterSpec.getProvider().endsWith("ec2")) {
      config.setProperty("fs.s3.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3.awsSecretAccessKey", clusterSpec.getCredential());
      config.setProperty("fs.s3n.awsAccessKeyId", clusterSpec.getIdentity());
      config.setProperty("fs.s3n.awsSecretAccessKey", clusterSpec.getCredential());
    }
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
  
  private void createProxyScript(ClusterSpec clusterSpec, Cluster cluster) {
    File configDir = getConfigDir(clusterSpec);
    File hadoopProxyFile = new File(configDir, "hadoop-proxy.sh");
    try {
      HadoopProxy proxy = new HadoopProxy(clusterSpec, cluster);
      InetAddress namenode = HadoopCluster.getNamenodePublicAddress(cluster);
      String script = String.format("echo 'Running proxy to Hadoop cluster at %s. " +
          "Use Ctrl-c to quit.'\n",
          DnsUtil.resolveAddress(namenode.getHostAddress()))
          + Joiner.on(" ").join(proxy.getProxyCommand());
      Files.write(script, hadoopProxyFile, Charsets.UTF_8);
      LOG.info("Wrote Hadoop proxy script {}", hadoopProxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hadoop proxy script {}", hadoopProxyFile, e);
    }
  }

}
