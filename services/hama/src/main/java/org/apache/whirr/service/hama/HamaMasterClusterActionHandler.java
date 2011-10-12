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
package org.apache.whirr.service.hama;

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Map.Entry;

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.FirewallManager.Rule;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

public class HamaMasterClusterActionHandler extends HamaClusterActionHandler {
  private static final Logger LOG = LoggerFactory
      .getLogger(HamaMasterClusterActionHandler.class);

  public static final String ROLE = "hama-master";
  public static final int MASTER_PORT = 40000;
  public static final int MASTER_WEB_UI_PORT = 40013;

  @Override
  public String getRole() {
    return ROLE;
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
      InterruptedException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Authorizing firewall");
    Instance instance = cluster.getInstanceMatching(role(ROLE));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    event.getFirewallManager().addRules(
        Rule.create().destination(instance).ports(MASTER_WEB_UI_PORT),
        Rule.create().destination(instance).ports(MASTER_PORT));

    String hamaConfigureFunction = getConfiguration(clusterSpec).getString(
        HamaConstants.KEY_CONFIGURE_FUNCTION,
        HamaConstants.FUNCTION_POST_CONFIGURE);

    String master = masterPublicAddress.getHostName();
    String quorum = ZooKeeperCluster.getHosts(cluster);

    String tarurl = prepareRemoteFileUrl(event, getConfiguration(clusterSpec)
        .getString(HamaConstants.KEY_TARBALL_URL));

    addStatement(event, call(hamaConfigureFunction, ROLE,
        HamaConstants.PARAM_MASTER, master, HamaConstants.PARAM_QUORUM, quorum,
        HamaConstants.PARAM_TARBALL_URL, tarurl));

    String hamaStartFunction = getConfiguration(clusterSpec).getString(
        HamaConstants.KEY_START_FUNCTION, HamaConstants.FUNCTION_START);

    addStatement(event, call(hamaStartFunction, ROLE,
        HamaConstants.PARAM_TARBALL_URL, tarurl));
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();

    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    Instance instance = cluster.getInstanceMatching(role(ROLE));
    InetAddress masterPublicAddress = instance.getPublicAddress();

    LOG.info("BSPMaster web UI available at http://{}:{}",
        masterPublicAddress.getHostName(), MASTER_WEB_UI_PORT);
    
    String quorum = ZooKeeperCluster.getHosts(cluster);
    Properties config = createClientSideProperties(masterPublicAddress, quorum);
    createClientSideHadoopSiteFile(clusterSpec, config);
    createProxyScript(clusterSpec, cluster);
    event.setCluster(new Cluster(cluster.getInstances(), config));
  }

  private Properties createClientSideProperties(InetAddress master,
      String quorum) throws IOException {
    Properties config = new Properties();
    config.setProperty(HamaConstants.PROP_HAMA_ZOOKEEPER_QUORUM, quorum);
    config.setProperty(HamaConstants.PROP_HAMA_ZOOKEEPER_CLIENTPORT, "2181");

    config.setProperty("bsp.master.address", master.getHostName() + ":"
        + MASTER_PORT);
    config.setProperty("hadoop.socks.server", "localhost:6666");
    config.setProperty("hadoop.rpc.socket.factory.class.default",
        "org.apache.hadoop.net.SocksSocketFactory");
    config.setProperty("hadoop.rpc.socket.factory.class.JobSubmissionProtocol",
        "org.apache.hadoop.net.StandardSocketFactory");

    return config;
  }

  private void createClientSideHadoopSiteFile(ClusterSpec clusterSpec,
      Properties config) {
    File configDir = getConfigDir(clusterSpec);
    File hamaSiteFile = new File(configDir, HamaConstants.FILE_HAMA_SITE_XML);
    try {
      Files.write(generateHamaConfigurationFile(config), hamaSiteFile,
          Charsets.UTF_8);
      LOG.info("Wrote Hama site file {}", hamaSiteFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hama site file {}", hamaSiteFile, e);
    }
  }

  private File getConfigDir(ClusterSpec clusterSpec) {
    File configDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    configDir = new File(configDir, clusterSpec.getClusterName());
    configDir.mkdirs();
    return configDir;
  }

  private CharSequence generateHamaConfigurationFile(Properties config) {
    StringBuilder sb = new StringBuilder();
    sb.append("<?xml version=\"1.0\"?>\n");
    sb
        .append("<?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?>\n");
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
    File hamaProxyFile = new File(configDir, "hama-proxy.sh");
    try {
      HadoopProxy proxy = new HadoopProxy(clusterSpec, cluster);
      InetAddress master = HamaCluster.getMasterPublicAddress(cluster);
      String script = String.format(
          "echo 'Running proxy to Hama cluster at %s. "
              + "Use Ctrl-c to quit.'\n", master.getHostName())
          + Joiner.on(" ").join(proxy.getProxyCommand());
      Files.write(script, hamaProxyFile, Charsets.UTF_8);
      LOG.info("Wrote Hama proxy script {}", hamaProxyFile);
    } catch (IOException e) {
      LOG.error("Problem writing Hama proxy script {}", hamaProxyFile, e);
    }
  }
}
