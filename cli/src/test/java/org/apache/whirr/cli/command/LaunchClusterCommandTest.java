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

package org.apache.whirr.cli.command;

import com.google.common.collect.Lists;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.apache.whirr.util.KeyPair;
import org.hamcrest.MatcherAssert;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import joptsimple.OptionSet;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LaunchClusterCommandTest extends BaseCommandTest{

  @Test
  public void testInsufficientArgs() throws Exception {
    LaunchClusterCommand command = new LaunchClusterCommand();
    Map<String, File> keys = KeyPair.generateTemporaryFiles();
    int rc = command.run(null, null, err, Lists.<String>newArrayList(
        "--private-key-file", keys.get("private").getAbsolutePath())
    );
    assertThat(rc, is(-1));
    assertThat(errBytes.toString(), containsString("Option 'cluster-name' not set."));
  }
  
  @Test
  public void testAllOptions() throws Exception {
    
    ClusterControllerFactory factory = mock(ClusterControllerFactory.class);
    ClusterController controller = mock(ClusterController.class);
    Cluster cluster = mock(Cluster.class);
    when(factory.create((String) any())).thenReturn(controller);
    when(controller.launchCluster((ClusterSpec) any())).thenReturn(cluster);
    
    LaunchClusterCommand command = new LaunchClusterCommand(factory);
    Map<String, File> keys = KeyPair.generateTemporaryFiles();
    
    int rc = command.run(null, out, null, Lists.newArrayList(
        "--service-name", "test-service",
        "--cluster-name", "test-cluster",
        "--instance-templates", "1 role1+role2,2 role3",
        "--provider", "rackspace",
        "--identity", "myusername", "--credential", "mypassword",
        "--private-key-file", keys.get("private").getAbsolutePath(),
        "--version", "version-string"
        ));
    
    assertThat(rc, is(0));

    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.version", "version-string");

    ClusterSpec expectedClusterSpec = ClusterSpec.withTemporaryKeys(conf);
    expectedClusterSpec.setInstanceTemplates(Lists.newArrayList(
      InstanceTemplate.builder().numberOfInstance(1).roles("role1", "role2").build(),
      InstanceTemplate.builder().numberOfInstance(2).roles("role3").build()
    ));
    expectedClusterSpec.setServiceName("test-service");
    expectedClusterSpec.setProvider("rackspace");
    expectedClusterSpec.setIdentity("myusername");
    expectedClusterSpec.setCredential("mypassword");
    expectedClusterSpec.setClusterName("test-cluster");
    expectedClusterSpec.setPrivateKey(keys.get("private"));
    expectedClusterSpec.setPublicKey(keys.get("public"));
    
    verify(factory).create("test-service");
    
    verify(controller).launchCluster(expectedClusterSpec);
    
    assertThat(outBytes.toString(), containsString("Started cluster of 0 instances"));
    
  }
  
  @Test
  public void testMaxPercentFailure() throws Exception {
    
    ClusterControllerFactory factory = mock(ClusterControllerFactory.class);
    ClusterController controller = mock(ClusterController.class);
    Cluster cluster = mock(Cluster.class);
    when(factory.create((String) any())).thenReturn(controller);
    when(controller.launchCluster((ClusterSpec) any())).thenReturn(cluster);
    
    LaunchClusterCommand command = new LaunchClusterCommand(factory);
    Map<String, File> keys = KeyPair.generateTemporaryFiles();
    
    int rc = command.run(null, out, null, Lists.newArrayList(
        "--service-name", "hadoop",
        "--cluster-name", "test-cluster",
        "--instance-templates", "1 hadoop-namenode+hadoop-jobtracker,3 hadoop-datanode+hadoop-tasktracker",
        "--instance-templates-max-percent-failures", "60 hadoop-datanode+hadoop-tasktracker",
        "--provider", "aws-ec2",
        "--identity", "myusername", "--credential", "mypassword",
        "--private-key-file", keys.get("private").getAbsolutePath(),
        "--version", "version-string"
        ));
    
    assertThat(rc, is(0));

    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.version", "version-string");
    conf.addProperty("whirr.instance-templates-max-percent-failure", "60 hadoop-datanode+hadoop-tasktracker");

    ClusterSpec expectedClusterSpec = ClusterSpec.withTemporaryKeys(conf);
    expectedClusterSpec.setInstanceTemplates(Lists.newArrayList(
      InstanceTemplate.builder().numberOfInstance(1).minNumberOfInstances(1)
        .roles("hadoop-namenode", "hadoop-jobtracker").build(),
      InstanceTemplate.builder().numberOfInstance(3).minNumberOfInstances(2)
        .roles("hadoop-datanode", "hadoop-tasktracker").build()
    ));
    expectedClusterSpec.setServiceName("hadoop");
    expectedClusterSpec.setProvider("aws-ec2");
    expectedClusterSpec.setIdentity("myusername");
    expectedClusterSpec.setCredential("mypassword");
    expectedClusterSpec.setClusterName("test-cluster");
    expectedClusterSpec.setPrivateKey(keys.get("private"));
    expectedClusterSpec.setPublicKey(keys.get("public"));
    
    verify(factory).create("hadoop");
    
    verify(controller).launchCluster(expectedClusterSpec);
    
    assertThat(outBytes.toString(), containsString("Started cluster of 0 instances")); 
  }

  static class TestLaunchClusterCommand extends LaunchClusterCommand {
    private ClusterSpec clusterSpec;
    private DryRun dryRun;

    public TestLaunchClusterCommand(ClusterControllerFactory factory) {
      super(factory);
    }

    @Override
    protected ClusterSpec getClusterSpec(OptionSet optionSet) throws ConfigurationException {
      return this.clusterSpec = super.getClusterSpec(optionSet);
    }

    @Override
    protected ClusterController createClusterController(String serviceName) {
      ClusterController controller = super.createClusterController(serviceName);
      this.dryRun = controller.getCompute().apply(clusterSpec).utils().injector().getInstance(DryRun.class).reset();
      return controller;
    }

  };

  @Test
  public void testLaunchClusterUsingDryRun() throws Exception {

    ClusterControllerFactory factory = new ClusterControllerFactory();
    TestLaunchClusterCommand launchCluster = new TestLaunchClusterCommand(factory);

    Map<String, File> keys = KeyPair.generateTemporaryFiles();

    int rc = launchCluster.run(null, out, err, Lists.<String>newArrayList(
        "--cluster-name", "test-cluster-launch",
        "--state-store", "none",
        "--instance-templates", "1 zookeeper+cassandra, 1 zookeeper+elasticsearch",
        "--provider", "stub",
        "--identity", "dummy",
        "--private-key-file", keys.get("private").getAbsolutePath()
    ));

    MatcherAssert.assertThat(rc, is(0));
    assertExecutedPhases(launchCluster.dryRun, "bootstrap", "configure", "start");
  }
}
