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

package org.apache.whirr;

import com.google.common.collect.Sets;
import org.jclouds.domain.Credentials;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.whirr.RolePredicates.role;
import static org.apache.whirr.RolePredicates.withIds;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

public class ClusterTest {

  private Cluster cluster;
  private static final int NUMBER_OF_INSTANCES = 5;

  @Before
  public void setUp() {
    Credentials credentials = new Credentials("dummy", "dummy");
    Set<Cluster.Instance> instances = Sets.newHashSet();

    for(int i = 0; i < NUMBER_OF_INSTANCES; i++) {
      String ip = "127.0.0." + (i + 1);
      instances.add(new Cluster.Instance(credentials,
        Sets.newHashSet("role-" + i), ip, ip, "id-" + i, null));
    }

    this.cluster = new Cluster(instances);
  }

  @Test
  public void remoteInstanceById() {
    cluster.removeInstancesMatching(withIds("id-0"));
    try {
      cluster.getInstanceMatching(withIds("id-0"));
      fail("Element not remnoved");
    } catch(NoSuchElementException e) {
      /* exception thrown as expected */
    }
    assertThat(cluster.getInstances().size(), is(NUMBER_OF_INSTANCES - 1));
  }

  @Test
  public void getInstanceById() {
    Cluster.Instance instance = cluster.getInstanceMatching(withIds("id-0"));
    assertThat(instance.getRoles().contains("role-0"), is(true));
  }

  @Test
  public void getInstanceByRole() {
    Cluster.Instance instance = cluster.getInstanceMatching(role("role-0"));
    assertThat(instance.getId(), is("id-0"));
  }

}
