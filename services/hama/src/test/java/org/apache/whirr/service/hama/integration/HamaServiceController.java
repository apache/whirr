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
package org.apache.whirr.service.hama.integration;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hama.HamaConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;

public class HamaServiceController {

  private static final Logger LOG = LoggerFactory
      .getLogger(HamaServiceController.class);

  private static final HamaServiceController INSTANCE = new HamaServiceController();

  public static HamaServiceController getInstance() {
    return INSTANCE;
  }

  private boolean running;
  private ClusterSpec clusterSpec;
  private ClusterController controller;
  private HadoopProxy proxy;
  private Cluster cluster;

  private HamaServiceController() {
  }

  public synchronized boolean ensureClusterRunning() throws Exception {
    if (running) {
      LOG.info("Cluster already running.");
      return false;
    } else {
      startup();
      return true;
    }
  }

  public synchronized void startup() throws Exception {
    LOG.info("Starting up cluster...");
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-hama-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    
    cluster = controller.launchCluster(clusterSpec);
    proxy = new HadoopProxy(clusterSpec, cluster);
    proxy.start();

    HamaConfiguration conf = getConfiguration();
    BSPJobClient client = new BSPJobClient(conf);
    waitForGroomServers(client);
    
    running = true;
  }

  private static void waitForGroomServers(BSPJobClient client) throws IOException {
    while (true) {
      ClusterStatus clusterStatus = client.getClusterStatus(true);
      int grooms = clusterStatus.getGroomServers();
      if (grooms > 0) {
        LOG.info("{} groomservers reported in. Continuing.", grooms);
        break;
      }
      try {
        System.out.print(".");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

  public HamaConfiguration getConfiguration() {
    HamaConfiguration conf = new HamaConfiguration();
    for (Entry<Object, Object> entry : cluster.getConfiguration().entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }

  public synchronized void shutdown() throws IOException, InterruptedException {
    LOG.info("Shutting down cluster...");
    if (proxy != null) {
      proxy.stop();
    }
    if (controller != null) {
      controller.destroyCluster(clusterSpec);
    }
    running = false;
  }
}
