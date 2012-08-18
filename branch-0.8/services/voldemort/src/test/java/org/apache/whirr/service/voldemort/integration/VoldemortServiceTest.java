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

package org.apache.whirr.service.voldemort.integration;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import voldemort.client.protocol.admin.AdminClient;
import voldemort.client.protocol.admin.AdminClientConfig;
import voldemort.cluster.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.apache.whirr.service.voldemort.VoldemortConstants.ADMIN_PORT;

public class VoldemortServiceTest {

  private ClusterSpec clusterSpec;

  private ClusterController controller;

  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-voldemort-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);

    controller = new ClusterController();
    cluster = controller.launchCluster(clusterSpec);

    waitForBootstrap();
  }

  private void waitForBootstrap() {
    for (Instance instance : cluster.getInstances()) {
      while (true) {
        try {
          String url = "tcp://" + instance.getPublicAddress().getHostAddress() + ":" + ADMIN_PORT;
          AdminClient client = new AdminClient(url, new AdminClientConfig());
          client.getAdminClientCluster();
          break;
        } catch (Exception e) {
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

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testInstances() throws Exception {
    Set<Instance> instances = cluster.getInstances();
    String url = "tcp://" + instances.iterator().next().getPublicAddress().getHostAddress() + ":" + ADMIN_PORT;
    AdminClient client = new AdminClient(url, new AdminClientConfig());
    voldemort.cluster.Cluster c = client.getAdminClientCluster();
  
    List<String> voldemortBasedHosts = new ArrayList<String>();

    for (Node node : c.getNodes()) 
      voldemortBasedHosts.add(node.getHost());

    List<String> whirrBasedHosts = new ArrayList<String>();

    for (Instance instance : instances) 
      whirrBasedHosts.add(instance.getPrivateAddress().getHostAddress());

    Collections.sort(voldemortBasedHosts);
    Collections.sort(whirrBasedHosts);

    Assert.assertEquals(whirrBasedHosts, voldemortBasedHosts);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (controller != null) {
      controller.destroyCluster(clusterSpec);
    }
  }

}
