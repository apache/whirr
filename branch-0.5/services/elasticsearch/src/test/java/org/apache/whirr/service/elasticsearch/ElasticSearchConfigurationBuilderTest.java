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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.junit.Test;

public class ElasticSearchConfigurationBuilderTest {

  @Test
  public void testGenerateYamlConfig() {
    Configuration defaults = new PropertiesConfiguration();

    defaults.addProperty("cloud.aws.id", "a");
    defaults.addProperty("cloud.aws.key", "b");
    defaults.addProperty("index.store.type", "memory");

    String content = StringUtils.join(
      ElasticSearchConfigurationBuilder.asYamlLines(defaults), "\n");

    assertThat(content, is("cloud:\n" +
      "  aws:\n" +
      "    id: a\n" +
      "    key: b\n" +
      "index:\n" +
      "  store:\n" +
      "    type: memory"));
  }

  @Test
  public void testDefaultConfigAwsEC2() throws Exception {
    Configuration baseConfig = new PropertiesConfiguration();
    baseConfig.addProperty("whirr.provider", "aws-ec2");
    baseConfig.addProperty("es.plugins", "lang-javascript, lang-python");

    ClusterSpec spec = ClusterSpec.withTemporaryKeys(baseConfig);
    Configuration config = ElasticSearchConfigurationBuilder.buildConfig(spec, null);

    assertThat(config.getStringArray("es.plugins"),
      is(new String[]{"lang-javascript", "lang-python", "cloud-aws"}));
    assertThat(config.getString("es.discovery.type"), is("ec2"));
  }

  @Test
  public void testDefaultUnicastConfig() throws Exception {
    Configuration baseConfig = new PropertiesConfiguration();
    baseConfig.addProperty("whirr.provider", "cloudservers-us");

    ClusterSpec spec = ClusterSpec.withTemporaryKeys(baseConfig);
    Cluster cluster = mock(Cluster.class);

    Set<Cluster.Instance> instances = Sets.newLinkedHashSet();
    for(String host : Lists.newArrayList("10.0.0.1", "10.0.0.2")) {
      Cluster.Instance instance = mock(Cluster.Instance.class);
      when(instance.getPrivateIp()).thenReturn(host);
      instances.add(instance);
    }
    when(cluster.getInstancesMatching((Predicate<Cluster.Instance>)any()))
      .thenReturn(instances);

    Configuration config = ElasticSearchConfigurationBuilder.buildConfig(spec, cluster);
    String content = StringUtils.join(
      ElasticSearchConfigurationBuilder.asYamlLines(
        config.subset(ElasticSearchConfigurationBuilder.ES_PREFIX)), "\n");

    assertThat(content, is("index:\n" +
      "  store:\n" +
      "    type: memory\n" +
      "gateway:\n" +
      "  type: none\n" +
      "discovery:\n" +
      "  zen:\n" +
      "    ping:\n" +
      "      multicast:\n" +
      "        enabled: false\n" +
      "      unicast:\n" +
      "        hosts: [\"10.0.0.1:9300\", \"10.0.0.2:9300\"]"));
  }

  @Test
  public void testOverrideDefaults() throws Exception {
    Configuration baseConfig = new PropertiesConfiguration();
    baseConfig.addProperty("whirr.provider", "aws-ec2");
    baseConfig.addProperty("es.index.store.type", "fs");

    ClusterSpec spec = ClusterSpec.withTemporaryKeys(baseConfig);
    Configuration config = ElasticSearchConfigurationBuilder.buildConfig(spec, null);

    assertThat(config.getString("es.index.store.type"), is("fs"));
  }
}
