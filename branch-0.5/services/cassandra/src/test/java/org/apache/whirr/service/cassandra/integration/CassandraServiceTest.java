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

import static org.junit.Assert.assertTrue;

import com.google.common.collect.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.cassandra.CassandraClusterActionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CassandraServiceTest {
   
  private ClusterSpec clusterSpec;
  private ClusterController controller;
  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-cassandra-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    
    controller = new ClusterController();
    cluster = controller.launchCluster(clusterSpec);

    // give it a sec to boot up the cluster
    waitForCassandra();
  }

  private Cassandra.Client client(Instance instance) throws TException
  {
    TTransport trans = new TFramedTransport(new TSocket(
        instance.getPublicIp(),
        CassandraClusterActionHandler.CLIENT_PORT));
    trans.open();
    TBinaryProtocol protocol = new TBinaryProtocol(trans);
    return new Cassandra.Client(protocol);
  }

  private void waitForCassandra() {
    for (Instance instance : cluster.getInstances()) {
      while (true) {
        try {
          Cassandra.Client client = client(instance);
          client.describe_cluster_name();
          client.getOutputProtocol().getTransport().close();
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
    Set<String> endPoints = Sets.newLinkedHashSet();
    for (Instance instance : cluster.getInstances()) {
      Cassandra.Client client = client(instance);
      Map<String,List<String>> tr = client.describe_schema_versions();
      for (List<String> version : tr.values()) {
        endPoints.addAll(version);
      }
      client.getOutputProtocol().getTransport().close();
    }
    
    for (Instance instance : cluster.getInstances()) {
      String address = instance.getPrivateAddress().getHostAddress();
      assertTrue(address + " not in cluster!", endPoints.remove(address));
    }
    assertTrue("Unknown node returned: " + endPoints.toString(), endPoints.isEmpty());
  }
  
  @After
  public void tearDown() throws IOException, InterruptedException {
    if (controller != null) {
      controller.destroyCluster(clusterSpec);      
    }
  }

}
