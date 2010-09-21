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

package org.apache.whirr.service.cassandra.integration;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.TokenRange;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.ServiceFactory;
import org.apache.whirr.service.Cluster.Instance;
import org.apache.whirr.service.cassandra.CassandraService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraServiceTest {

  private static final String KEYSPACE = "Keyspace1";

  private ClusterSpec clusterSpec;
  private CassandraService service;
  private Cluster cluster;

  @Before
  public void setUp() throws ConfigurationException, IOException {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-cassandra-test.properties"));
    clusterSpec = new ClusterSpec(config);
    Service s = new ServiceFactory().create(clusterSpec.getServiceName());
    assertThat(s, instanceOf(CassandraService.class));
    service = (CassandraService) s;
    cluster = service.launchCluster(clusterSpec);

    // give it a sec to boot up the cluster
    waitForCassandra();
  }

  private void waitForCassandra() {
    for (Instance instance : cluster.getInstances()) {
      while (true) {
        try {
          TSocket socket = new TSocket(instance.getPublicAddress()
              .getHostAddress(), CassandraService.CLIENT_PORT);
          socket.open();
          TBinaryProtocol protocol = new TBinaryProtocol(socket);
          Cassandra.Client client = new Cassandra.Client(protocol);
          client.describe_cluster_name();
          socket.close();
          break;
        } catch (TException e) {
          System.out.print(".");
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e1) {
            break;
          }
        }
      }
    }
  }

  @Test
  public void testInstances() throws Exception {
    Set<String> endPoints = new HashSet<String>();
    for (Instance instance : cluster.getInstances()) {
      TSocket socket = new TSocket(instance.getPublicAddress().getHostAddress(), 
          CassandraService.CLIENT_PORT);
      socket.open();
      TBinaryProtocol protocol = new TBinaryProtocol(socket);
      Cassandra.Client client = new Cassandra.Client(protocol);
      List<TokenRange> tr = client.describe_ring(KEYSPACE);
      for (TokenRange tokenRange : tr) {
        endPoints.addAll(tokenRange.endpoints);
      }
      socket.close();
    }
    
    for (Instance instance : cluster.getInstances()) {
      String address = instance.getPrivateAddress().getHostAddress();
      assertTrue(address + " not in cluster!", endPoints.remove(address));
    }
    assertTrue("Unknown node returned: " + endPoints.toString(), endPoints.isEmpty());
  }
  
  @After
  public void tearDown() throws IOException {
    if (service != null) {
      service.destroyCluster(clusterSpec);      
    }
  }

}
