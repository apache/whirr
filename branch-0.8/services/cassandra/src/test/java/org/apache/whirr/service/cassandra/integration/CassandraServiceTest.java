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

import com.google.common.collect.Sets;
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
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.cassandra.CassandraClusterActionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class CassandraServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(CassandraServiceTest.class);
   
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

    waitForCassandra();
  }

  private Cassandra.Client client(Instance instance) throws TException {
    TTransport trans = new TFramedTransport(new TSocket(
        instance.getPublicIp(),
        CassandraClusterActionHandler.CLIENT_PORT));
    trans.open();
    TBinaryProtocol protocol = new TBinaryProtocol(trans);
    return new Cassandra.Client(protocol);
  }

  private void waitForCassandra() {
    LOG.info("Waiting for Cassandra to start");
    for (Instance instance : cluster.getInstances()) {
      int tries = 0;
      while (tries < 30) {
        try {
          Cassandra.Client client = client(instance);
          client.describe_cluster_name();
          client.getOutputProtocol().getTransport().close();
          LOG.info(instance.getPublicIp() + " is up and running");
          break;

        } catch (TException e) {
          try {
            LOG.warn(instance.getPublicIp() + " not reachable, try #" + tries + ", waiting 1s");
            Thread.sleep(10000);
          } catch (InterruptedException e1) {
            break;
          }
          tries += 1;
        }
      }
      if (tries == 10) {
        LOG.error("Instance " + instance.getPublicIp() + " is still unavailable after 10 retries");
      }
    }
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
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
    LOG.info("List of endpoints: " + endPoints);
    
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
