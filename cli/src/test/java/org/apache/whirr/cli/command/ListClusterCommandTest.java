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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceFactory;
import org.apache.whirr.ssh.KeyPair;
import org.hamcrest.Matcher;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.compute.domain.internal.NodeMetadataImpl;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.internal.LocationImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.StringContains;

public class ListClusterCommandTest {

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
  public void testInsufficientOptions() throws Exception {
    ListClusterCommand command = new ListClusterCommand();
    int rc = command.run(null, null, err, Collections.<String>emptyList());
    assertThat(rc, is(-1));
    assertThat(errBytes.toString(), containsUsageString());
  }
  
  private Matcher<String> containsUsageString() {
    return StringContains.containsString("Usage: whirr list-cluster [OPTIONS]");
  }
  
  @Test
  public void testAllOptions() throws Exception {
    
    ServiceFactory factory = mock(ServiceFactory.class);
    Service service = mock(Service.class);
    when(factory.create((String) any())).thenReturn(service);
    NodeMetadata node1 = new NodeMetadataBuilder().name("name1").ids("id1")
        .location(new LocationBuilder().scope(LocationScope.PROVIDER)
          .id("location-id1").description("location-desc1").build())
        .imageId("image-id").state(NodeState.RUNNING)
        .publicAddresses(Lists.newArrayList("100.0.0.1"))
        .privateAddresses(Lists.newArrayList("10.0.0.1")).build();
    NodeMetadata node2 = new NodeMetadataBuilder().name("name2").ids("id2")
        .location(new LocationBuilder().scope(LocationScope.PROVIDER)
          .id("location-id2").description("location-desc2").build())
        .imageId("image-id").state(NodeState.RUNNING)
        .publicAddresses(Lists.newArrayList("100.0.0.2"))
        .privateAddresses(Lists.newArrayList("10.0.0.2")).build();
    when(service.getNodes((ClusterSpec) any())).thenReturn(
        (Set) Sets.newLinkedHashSet(Lists.newArrayList(node1, node2)));

    ListClusterCommand command = new ListClusterCommand(factory);

    Map<String, File> keys = KeyPair.generateTemporaryFiles();
    int rc = command.run(null, out, null, Lists.newArrayList(
        "--service-name", "test-service",
        "--cluster-name", "test-cluster",
        "--identity", "myusername",
        "--private-key-file", keys.get("private").getAbsolutePath())
    );
    
    assertThat(rc, is(0));
    
    assertThat(outBytes.toString(), is(
      "id1\timage-id\t100.0.0.1\t10.0.0.1\tRUNNING\tlocation-id1\n" +
      "id2\timage-id\t100.0.0.2\t10.0.0.2\tRUNNING\tlocation-id2\n"));
    
    verify(factory).create("test-service");
    
  }
}
