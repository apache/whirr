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

import static com.google.common.base.Preconditions.checkArgument;
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

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.cli.MemoryClusterStateStore;
import org.apache.whirr.service.ClusterStateStore;
import org.apache.whirr.service.ClusterStateStoreFactory;
import org.apache.whirr.util.KeyPair;
import org.hamcrest.Matcher;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
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

    ClusterControllerFactory factory = mock(ClusterControllerFactory.class);
    ClusterController controller = mock(ClusterController.class);
    when(factory.create((String) any())).thenReturn(controller);

    NodeMetadata node1 = new NodeMetadataBuilder().name("name1").ids("id1")
        .location(new LocationBuilder().scope(LocationScope.PROVIDER)
          .id("location-id1").description("location-desc1").build())
        .imageId("image-id").state(NodeState.RUNNING)
        .publicAddresses(Lists.newArrayList("127.0.0.1"))
        .privateAddresses(Lists.newArrayList("127.0.0.1")).build();

    NodeMetadata node2 = new NodeMetadataBuilder().name("name2").ids("id2")
        .location(new LocationBuilder().scope(LocationScope.PROVIDER)
          .id("location-id2").description("location-desc2").build())
        .imageId("image-id").state(NodeState.RUNNING)
        .publicAddresses(Lists.newArrayList("127.0.0.2"))
        .privateAddresses(Lists.newArrayList("127.0.0.2")).build();

    when(controller.getNodes((ClusterSpec) any())).thenReturn(
        (Set) Sets.newLinkedHashSet(Lists.newArrayList(node1, node2)));
    when(controller.getInstances((ClusterSpec)any(), (ClusterStateStore)any()))
        .thenCallRealMethod();

    ClusterStateStore memStore = new MemoryClusterStateStore();
    memStore.save(createTestCluster(
      new String[]{"id1", "id2"}, new String[]{"role1", "role2"}));

    ClusterStateStoreFactory stateStoreFactory = mock(ClusterStateStoreFactory.class);
    when(stateStoreFactory.create((ClusterSpec) any())).thenReturn(memStore);

    ListClusterCommand command = new ListClusterCommand(factory, stateStoreFactory);

    Map<String, File> keys = KeyPair.generateTemporaryFiles();
    int rc = command.run(null, out, null, Lists.newArrayList(
        "--instance-templates", "1 noop",
        "--service-name", "test-service",
        "--cluster-name", "test-cluster",
        "--identity", "myusername",
        "--private-key-file", keys.get("private").getAbsolutePath())
    );
    
    assertThat(rc, is(0));
    
    assertThat(outBytes.toString(), is(
      "id1\timage-id\t127.0.0.1\t127.0.0.1\tRUNNING\tlocation-id1\trole1\n" +
      "id2\timage-id\t127.0.0.2\t127.0.0.2\tRUNNING\tlocation-id2\trole2\n"));
    
    verify(factory).create("test-service");
    
  }

  private Cluster createTestCluster(String[] ids, String[] roles) {
    checkArgument(ids.length == roles.length, "each ID should have a role");

    Credentials credentials = new Credentials("dummy", "dummy");
    Set<Cluster.Instance> instances = Sets.newHashSet();

    for(int i = 0; i < ids.length; i++) {
      String ip = "127.0.0." + (i + 1);
      instances.add(new Cluster.Instance(credentials,
        Sets.newHashSet(roles[i]), ip, ip, ids[i], null));
    }

    return new Cluster(instances);
  }


}
