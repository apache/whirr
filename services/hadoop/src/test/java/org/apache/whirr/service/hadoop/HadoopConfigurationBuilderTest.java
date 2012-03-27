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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Iterators;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.net.DnsResolver;
import org.apache.whirr.net.FakeDnsResolver;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.domain.internal.HardwareImpl;
import org.jclouds.domain.Credentials;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HadoopConfigurationBuilderTest {

  static class RegexMatcher extends BaseMatcher<String> {
    private final String regex;

    public RegexMatcher(String regex) {
      this.regex = regex;
    }

    public boolean matches(Object o) {
      return ((String) o).matches(regex);
    }

    public void describeTo(Description description) {
      description.appendText("matches regex=");
    }

  }
  
  static RegexMatcher matches(String regex) {
    return new RegexMatcher(regex);
  }
  
  private Configuration defaults;
  private ClusterSpec clusterSpec;
  private Cluster cluster;
  
  @Before
  public void setUp() throws Exception {
    defaults = new PropertiesConfiguration();
    defaults.addProperty("hadoop-common.p1", "common1");
    defaults.addProperty("hadoop-common.p2", "common2");
    defaults.addProperty("hadoop-hdfs.p1", "hdfs1");
    defaults.addProperty("hadoop-mapreduce.p1", "mapred1");

    clusterSpec = ClusterSpec.withTemporaryKeys();
    cluster = newCluster(1);
  }
  
  private Cluster newCluster(int numberOfWorkers) {
    DnsResolver fakeDnsResolver = new FakeDnsResolver();

    NodeMetadata node = mock(NodeMetadata.class);
    List<Processor> processors = ImmutableList.of(new Processor(4, 1.0));
    Hardware hardware = new HardwareImpl(null, null, "id", null, null,
        ImmutableMap.<String,String>of(), ImmutableSet.<String>of(), processors, 
        1024, ImmutableList.<Volume>of(), null, "xen");
    when(node.getHardware()).thenReturn(hardware);
    
    Builder<Instance> instances = ImmutableSet.<Instance>builder();
    Instance master = new Instance(new Credentials("", ""),
        ImmutableSet.of(HadoopNameNodeClusterActionHandler.ROLE,
            HadoopJobTrackerClusterActionHandler.ROLE),
            "10.0.0.1", "10.0.0.1", "1", node, fakeDnsResolver);
    instances.add(master);
    for (int i = 0; i < numberOfWorkers; i++) {
      int id = i + 2;
      instances.add(new Instance(new Credentials("", ""),
        ImmutableSet.of(HadoopDataNodeClusterActionHandler.ROLE,
              HadoopTaskTrackerClusterActionHandler.ROLE),
              "10.0.0." + id, "10.0.0." + id, id + "", node, fakeDnsResolver));
    }
    return new Cluster(instances.build());
  }

  @Test
  public void testCommon() throws Exception {
    Configuration conf = HadoopConfigurationBuilder.buildCommonConfiguration(
        clusterSpec, cluster, defaults);
    assertThat(Iterators.size(conf.getKeys()), is(3));
    assertThat(conf.getString("p1"), is("common1"));
    assertThat(conf.getString("p2"), is("common2"));
    assertThat(conf.getString("fs.default.name"), matches("hdfs://.+:8020/"));
  }
  
  @Test
  public void testOverrides() throws Exception {
    Configuration overrides = new PropertiesConfiguration();
    overrides.addProperty("hadoop-common.p1", "overridden1");
    overrides.addProperty("hadoop-common.p2", "overridden2");
    overrides.addProperty("hadoop-common.fs.default.name", "not-overridden");
    clusterSpec = ClusterSpec.withNoDefaults(overrides);
    Configuration conf = HadoopConfigurationBuilder.buildCommonConfiguration(
        clusterSpec, cluster, defaults);
    assertThat(Iterators.size(conf.getKeys()), is(3));
    assertThat(conf.getString("p1"), is("overridden1"));
    assertThat(conf.getString("p2"), is("overridden2"));
    assertThat("Can't override dynamically set properties",
        conf.getString("fs.default.name"), matches("hdfs://.+:8020/"));
  }

  @Test
  public void testHdfs() throws Exception {
    Configuration conf = HadoopConfigurationBuilder.buildHdfsConfiguration(
        clusterSpec, cluster, defaults);
    assertThat(Iterators.size(conf.getKeys()), is(1));
    assertThat(conf.getString("p1"), is("hdfs1"));
  }

  @Test
  public void testMapReduce() throws Exception {
    Cluster cluster = newCluster(5);
    Configuration conf = HadoopConfigurationBuilder
      .buildMapReduceConfiguration(clusterSpec, cluster, defaults);
    assertThat(conf.getString("p1"), is("mapred1"));
    assertThat(conf.getString("mapred.job.tracker"), matches(".+:8021"));
    assertThat(conf.getString("mapred.tasktracker.map.tasks.maximum"), is("4"));
    assertThat(conf.getString("mapred.tasktracker.reduce.tasks.maximum"), is("3"));
    assertThat(conf.getString("mapred.reduce.tasks"), is("15"));
  }

  @Test
  public void testOverridesNumberOfMappers() throws Exception {
    Configuration overrides = new PropertiesConfiguration();
    overrides.addProperty("hadoop-mapreduce.mapred.tasktracker.map.tasks.maximum", "70");
    clusterSpec = ClusterSpec.withNoDefaults(overrides);
    Configuration conf = HadoopConfigurationBuilder.buildMapReduceConfiguration(
        clusterSpec, cluster, defaults);
    assertThat(conf.getString("mapred.tasktracker.map.tasks.maximum"), is("70"));
  }
  
  @Test
  public void testOverridesNumberOfReducers() throws Exception {
    Configuration overrides = new PropertiesConfiguration();
    overrides.addProperty("hadoop-mapreduce.mapred.reduce.tasks", "7");
    clusterSpec = ClusterSpec.withNoDefaults(overrides);
    Configuration conf = HadoopConfigurationBuilder.buildMapReduceConfiguration(
        clusterSpec, cluster, defaults);
    assertThat(conf.getString("mapred.reduce.tasks"), is("7"));
  }


}
