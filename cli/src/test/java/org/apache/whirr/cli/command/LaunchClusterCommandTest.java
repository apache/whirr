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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;

import org.apache.whirr.cli.command.LaunchClusterCommand;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceFactory;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.StringContains;

import com.google.common.collect.Sets;
import com.google.inject.internal.Lists;

public class LaunchClusterCommandTest {

  private ByteArrayOutputStream outBytes;
  private PrintStream out;
  private ByteArrayOutputStream errBytes;
  private PrintStream err;

  @Before
  public void setUp() {
    outBytes = new ByteArrayOutputStream();
    out = new PrintStream(outBytes);
    errBytes = new ByteArrayOutputStream();
    err = new PrintStream(errBytes);
  }
  @Test
  public void testNoArgs() throws Exception {
    LaunchClusterCommand command = new LaunchClusterCommand();
    int rc = command.run(null, null, err, Collections.<String>emptyList());
    assertThat(rc, is(-1));
    assertThat(errBytes.toString(), containsUsageString());
  }
  
  @Test
  public void testMissingNumberAndRoles() throws Exception {
    LaunchClusterCommand command = new LaunchClusterCommand();
    int rc = command.run(null, null, err, Lists.newArrayList("test-service",
        "test-cluster"));
    assertThat(rc, is(-1));
    assertThat(errBytes.toString(), containsUsageString());
  }
  
  @Test
  public void testMissingRoles() throws Exception {
    LaunchClusterCommand command = new LaunchClusterCommand();
    int rc = command.run(null, null, err, Lists.newArrayList("test-service",
        "test-cluster", "1", "role1+role2", "2"));
    assertThat(rc, is(-1));
    assertThat(errBytes.toString(), containsUsageString());
  }
  
  private Matcher<String> containsUsageString() {
    return StringContains.containsString("Usage: whirr launch-cluster " +
        "[OPTIONS] <service-name> <cluster-name> <num> <roles> [<num> <roles>]*");
  }
  
  @Test
  public void testAllOptions() throws Exception {
    
    ServiceFactory factory = mock(ServiceFactory.class);
    Service service = mock(Service.class);
    Cluster cluster = mock(Cluster.class);
    when(factory.create((String) any())).thenReturn(service);
    when(service.launchCluster((ClusterSpec) any())).thenReturn(cluster);
    
    LaunchClusterCommand command = new LaunchClusterCommand(factory);
    
    int rc = command.run(null, out, null, Lists.newArrayList(
        "--cloud-provider", "rackspace",
        "--cloud-identity", "myusername", "--cloud-credential", "mypassword",
        "--secret-key-file", "secret-key",
        "test-service", "test-cluster", "1", "role1+role2", "2", "role3"));
    
    assertThat(rc, is(0));

    ClusterSpec expectedClusterSpec = new ClusterSpec(
        new ClusterSpec.InstanceTemplate(1, Sets.newHashSet("role1", "role2")),
        new ClusterSpec.InstanceTemplate(2, Sets.newHashSet("role3"))
    );
    expectedClusterSpec.setProvider("rackspace");
    expectedClusterSpec.setIdentity("myusername");
    expectedClusterSpec.setCredential("mypassword");
    expectedClusterSpec.setClusterName("test-cluster");
    expectedClusterSpec.setSecretKeyFile("secret-key");
    
    verify(factory).create("test-service");
    
    verify(service).launchCluster(expectedClusterSpec);
    
    assertThat(outBytes.toString(), containsString("Started cluster of 0 instances"));
    
  }
}
