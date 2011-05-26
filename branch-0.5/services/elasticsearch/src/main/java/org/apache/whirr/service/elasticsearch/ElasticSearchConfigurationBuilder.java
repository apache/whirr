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

package org.apache.whirr.service.elasticsearch;

import static org.apache.whirr.RolePredicates.role;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;
import org.slf4j.LoggerFactory;

public class ElasticSearchConfigurationBuilder {

  private static final org.slf4j.Logger LOG =
    LoggerFactory.getLogger(ElasticSearchConfigurationBuilder.class);

  public static final String ES_PREFIX = "es";

  /**
   * Generate an appendFile statement for jclouds
   */
  public static Statement build(String path, Configuration config) {
    return asFileStatement(path, config);
  }

  public static Statement build(String path, ClusterSpec spec, Cluster cluster) {
    return build(path, buildConfig(spec, cluster));
  }

  /**
   * Build a configuration by adding the expected defaults
   */
  public static Configuration buildConfig(ClusterSpec spec, Cluster cluster) {
    CompositeConfiguration config = new CompositeConfiguration();

    config.addConfiguration(spec.getConfiguration());
    try {
      config.addConfiguration(
        new PropertiesConfiguration("whirr-elasticsearch-default.properties"));
    } catch (ConfigurationException e) {
      LOG.error("Configuration error", e); // this should never happen
    }

    if ("aws-ec2".equals(spec.getProvider()) || "ec2".equals(spec.getProvider())) {
      addDefaultsForEC2(spec, config);
    } else {
      addDefaultsForUnicast(cluster, config);
    }
    if (!config.containsKey("es.cluster.name")) {
      config.addProperty("es.cluster.name", spec.getClusterName());
    }

    return config;
  }

  /**
   * Use the native EC2 discovery module on AWS
   */
  private static void addDefaultsForEC2(ClusterSpec spec, CompositeConfiguration config) {
    config.addProperty("es.discovery.type", "ec2");
    if (!config.containsKey("es.cloud.aws.access_key")) {
      config.addProperty("es.cloud.aws.access_key", spec.getIdentity());
    }
    if (!config.containsKey("es.cloud.aws.secret_key")) {
      config.addProperty("es.cloud.aws.secret_key", spec.getCredential());
    }
    if (!config.getList("es.plugins", Lists.newLinkedList()).contains("cloud-aws")) {
      config.addProperty("es.plugins", "cloud-aws");
    }
  }

  /**
   * Use unicast if not on AWS (most of the cloud providers deny multicast traffic).
   */
  private static void addDefaultsForUnicast(Cluster cluster, CompositeConfiguration config) {
    List<String> hosts = Lists.newLinkedList();
    for(Cluster.Instance instance : cluster.getInstancesMatching(role(ElasticSearchHandler.ROLE))) {
      hosts.add(String.format("\"%s:9300\"", instance.getPrivateIp()));
    }
    config.addProperty("es.discovery.zen.ping.multicast.enabled", "false");
    config.addProperty("es.discovery.zen.ping.unicast.hosts", StringUtils.join(hosts, ","));
  }

  private static Statement asFileStatement(String path, Configuration configuration) {
    return Statements.appendFile(path, asYamlLines(configuration.subset(ES_PREFIX)));
  }

  /**
   * Create the YAML configuration file lines from the configuration.
   *
   * This functions transforms cloud.aws.id=1 to
   *
   * cloud:
   *   aws:
   *     id: 1
   */
  @VisibleForTesting
  public static List<String> asYamlLines(Configuration config) {
    return asYamlLines(config, 0);
  }

  private static List<String> asYamlLines(Configuration config, int depth) {
    List<String> lines = Lists.newArrayList();
    Set<String> prefixes = Sets.newHashSet();

    Iterator<String> keys = config.getKeys();
    while(keys.hasNext()) {
      String key = keys.next();

      String[] parts = key.split("\\.");
      String prefix = parts[0];

      if (prefixes.contains(prefix)) {
        continue; // skip parsed set of keys
      }

      if (parts.length == 1) {
        lines.add(spaces(depth * 2) + key + ": " + config.getProperty(key));

      } else if (parts.length > 1) {
        lines.add(spaces(depth * 2) + prefix + ":");
        lines.addAll(asYamlLines(config.subset(prefix), depth + 1));
      }

      prefixes.add(prefix);
    }

    return lines;
  }

  /**
   * Generate a string with spaces having the requested length
   */
  private static String spaces(int length) {
    StringBuilder builder = new StringBuilder();
    for(int i=0; i<length; i++) {
      builder.append(" ");
    }
    return builder.toString();
  }
}
