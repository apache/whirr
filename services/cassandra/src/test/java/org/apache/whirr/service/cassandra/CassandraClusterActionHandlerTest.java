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

package org.apache.whirr.service.cassandra;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Sets;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import org.apache.whirr.Cluster.Instance;
import org.jclouds.domain.Credentials;
import org.junit.Test;

public class CassandraClusterActionHandlerTest {

  private Instance getInstance(String id) throws UnknownHostException {
    return new Instance(new Credentials("", ""), Sets.newHashSet(""),
        "127.0.0.1", "127.0.0.1", id, null); 
  }
  
  @Test()
  public void testGetSeeds() throws UnknownHostException {
    Instance one = getInstance("1");
    Instance two = getInstance("2");
    
    Set<Instance> instances = Sets.newLinkedHashSet();
    instances.add(one);
    
    CassandraClusterActionHandler handler = new CassandraClusterActionHandler();
    // check that the one node is returned
    List<Instance> seeds1 = handler.getSeeds(instances);
    assertEquals(1, seeds1.size());
    assertEquals(one, seeds1.get(0));
    
    // add more nodes, should get two seeds
    instances.add(two);
    for (int i = 3; i < 10; i++) {
      instances.add(getInstance(Integer.toString(i)));
    }
    
    List<Instance> seeds2 = handler.getSeeds(instances);
    assertEquals(2, seeds2.size());
    assertEquals(one, seeds2.get(0));
    assertEquals(two, seeds2.get(1));
  }
}
