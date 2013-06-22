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

package org.apache.whirr.service.solr;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.hash.Hashing.md5;
import static com.google.common.io.BaseEncoding.base16;
import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.FirewallManager.Rule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class SolrClusterActionHandler extends ClusterActionHandlerSupport {

  private static final Logger LOG = LoggerFactory.getLogger(SolrClusterActionHandler.class);

  public final static String SOLR_ROLE = "solr";

  final static String SOLR_DEFAULT_CONFIG = "whirr-solr-default.properties";

  final static String SOLR_HOME = "/usr/local/solr";

  final static String SOLR_TARBALL_URL = "whirr.solr.tarball";
  final static String SOLR_CONFIG_TARBALL_URL = "whirr.solr.config.tarball.url";
  final static String SOLR_JETTY_PORT = "whirr.solr.jetty.port";
  final static String SOLR_JETTY_STOP_PORT = "whirr.solr.jetty.stop.port";
  final static String SOLR_JETTY_STOP_SECRET = "whirr.solr.jetty.stop.secret";
  public static final String SOLR_JAVA_OPTS = "whirr.solr.java.opts";

  @Override
  public String getRole() {
    return SOLR_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    Configuration config = getConfiguration(event.getClusterSpec(), SOLR_DEFAULT_CONFIG);

    // Validate the config
    int jettyPort = config.getInt(SOLR_JETTY_PORT);
    int jettyStopPort = config.getInt(SOLR_JETTY_STOP_PORT);

    if (jettyPort == 0) {
      throw new IllegalArgumentException("Must specify Jetty's port! (" + SOLR_JETTY_PORT + ")");
    }

    if (jettyStopPort == 0) {
      throw new IllegalArgumentException("Must specify Jetty's stop port! (" + SOLR_JETTY_STOP_PORT + ")");
    }

    if (jettyPort == jettyStopPort) {
      throw new IllegalArgumentException("Jetty's port and the stop port must be different");
    }

    String solrConfigTarballUrl = config.getString(SOLR_CONFIG_TARBALL_URL);
    if (StringUtils.isBlank(solrConfigTarballUrl)) {
      throw new IllegalArgumentException("Must specify Solr config tarball! (" + SOLR_CONFIG_TARBALL_URL + ")");
    }

    String solrTarball = config.getString(SOLR_TARBALL_URL);
    if(!solrTarball.matches("^.*apache-solr-.*(tgz|tar\\.gz)$")) {
      throw new IllegalArgumentException("Must specify a Solr tarball");
    }
    // Call the installers
    addStatement(event, call(getInstallFunction(config, "java", "install_openjdk")));
    addStatement(event, call("install_tarball"));

    String installFunc = getInstallFunction(config, getRole(), "install_" + getRole());

    LOG.info("Installing Solr");

    addStatement(event, call(installFunc, solrTarball, SOLR_HOME));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException {
    LOG.info("Configure Solr");

    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration config = getConfiguration(clusterSpec, SOLR_DEFAULT_CONFIG);

    int jettyPort = config.getInt(SOLR_JETTY_PORT);

    // Open up Jetty port
    event.getFirewallManager().addRule(Rule.create().destination(role(SOLR_ROLE)).port(jettyPort));

    handleFirewallRules(event);
  }

  @Override
  protected void beforeStart(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration config = getConfiguration(clusterSpec, SOLR_DEFAULT_CONFIG);

    String solrConfigTarballUrl = prepareRemoteFileUrl(event, config.getString(SOLR_CONFIG_TARBALL_URL));
    LOG.info("Preparing solr config tarball url {}", solrConfigTarballUrl);
    addStatement(event, call("install_tarball_no_md5", solrConfigTarballUrl, SOLR_HOME));

    int jettyPort = config.getInt(SOLR_JETTY_PORT);
    int jettyStopPort = config.getInt(SOLR_JETTY_STOP_PORT);

    String startFunc = getStartFunction(config, getRole(), "start_" + getRole());
    LOG.info("Starting up Solr");

    addStatement(event, call(startFunc,
        String.valueOf(jettyPort),
        String.valueOf(jettyStopPort),
        safeSecretString(config.getString(SOLR_JETTY_STOP_SECRET)),
        SOLR_HOME,
        SOLR_HOME + "/example/start.jar",
        config.getString(SOLR_JAVA_OPTS, "")
    ));
  }

  @Override
  protected void afterStart(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Cluster cluster = event.getCluster();
    Configuration config = getConfiguration(clusterSpec, SOLR_DEFAULT_CONFIG);
    int jettyPort = config.getInt(SOLR_JETTY_PORT);
    LOG.info("Completed configuration of {}", clusterSpec.getClusterName());
    LOG.info("Solr Hosts: {}", getHosts(cluster.getInstancesMatching(role(SOLR_ROLE)), jettyPort));
  }

  @Override
  protected void beforeStop(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration config = getConfiguration(clusterSpec, SOLR_DEFAULT_CONFIG);
    int jettyStopPort = config.getInt(SOLR_JETTY_STOP_PORT);
    String stopFunc = getStopFunction(config, getRole(), "stop_" + getRole());
    LOG.info("Stopping Solr");
    addStatement(event, call(stopFunc,
      SOLR_HOME,
      String.valueOf(jettyStopPort),
      safeSecretString(config.getString(SOLR_JETTY_STOP_SECRET)),
      SOLR_HOME + "/example/start.jar"
    ));
  }

  static List<String> getHosts(Set<Instance> instances, final int port) {
    return Lists.transform(Lists.newArrayList(instances), new GetPublicIpFunction(port));
  }

  static String safeSecretString(String value) throws IOException {
    return md5Hex("NaCl#" + value);
  }

  private static class GetPublicIpFunction implements Function<Instance, String> {
    private final int port;

    public GetPublicIpFunction(int port) {
      this.port = port;
    }

    @Override
    public String apply(Instance instance) {
      try {
        String publicIp = instance.getPublicHostName();
        return String.format("%s:%d", publicIp, port);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  public static String md5Hex(String in) {
    return base16().lowerCase().encode(md5().hashString(in, UTF_8).asBytes());
  }
}
