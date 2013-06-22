/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.whirr.service.puppet.integration;

import static org.jclouds.util.Predicates2.retry;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.jclouds.predicates.InetSocketAddressConnect;
import org.jclouds.util.Strings2;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.net.HostAndPort;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

/**
 * Install an http service on the remote machine with puppet!
 */
public class PuppetServiceTest {

  private ClusterSpec clusterSpec;
  private ClusterController controller;
  private Cluster cluster;
  private Predicate<HostAndPort> socketTester;

  @Before
  public void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-puppet-test.properties"));

    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    cluster = controller.launchCluster(clusterSpec);
    socketTester = retry(controller.getCompute().apply(clusterSpec).utils().injector().getInstance(InetSocketAddressConnect.class),
                         60, 1, TimeUnit.SECONDS);
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testHttpAvailable() throws Exception {

    // check that the http server started
    for (Instance instance : cluster.getInstances()) {
      // first, check the socket
      HostAndPort socket = HostAndPort.fromParts(instance.getPublicAddress().getHostAddress(), 80);
      assert socketTester.apply(socket) : instance;
      
      // then, try a GET
      URI httpUrl = URI.create("http://" + instance.getPublicAddress().getHostAddress());
      Strings2.toStringAndClose(httpUrl.toURL().openStream());
    }

  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    if (controller != null) {
      controller.destroyCluster(clusterSpec);
    }
  }

}
