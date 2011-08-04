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

package org.apache.whirr.service.hbase.integration;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.IOError;
import org.apache.hadoop.hbase.thrift.generated.IllegalArgument;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.apache.whirr.service.hbase.HBaseThriftServerClusterActionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseServiceController {

  private final String configResource;

  private static final Logger LOG =
    LoggerFactory.getLogger(HBaseServiceController.class);

  private static final Map<String, HBaseServiceController> INSTANCES = new HashMap<String, HBaseServiceController>();

  public static HBaseServiceController getInstance(String configResource) {
    HBaseServiceController controller = INSTANCES.get(configResource);
    if (controller == null) {
      controller = new HBaseServiceController(configResource);
      INSTANCES.put(configResource, controller);
    }
    return controller;
  }

  private boolean running;
  private ClusterSpec clusterSpec;
  private ClusterController controller;
  private HadoopProxy proxy;
  private Cluster cluster;
  private Hbase.Client thriftClient;

  private HBaseServiceController(String configResource) {
    this.configResource = configResource;
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
    config.addConfiguration(new PropertiesConfiguration(this.configResource));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();

    cluster = controller.launchCluster(clusterSpec);
    proxy = new HadoopProxy(clusterSpec, cluster);
    proxy.start();

    waitForMaster();
    running = true;
  }

  public Configuration getConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    for (Entry<Object, Object> entry : cluster.getConfiguration().entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }

  private void waitForMaster()
  throws IOException, TException, IOError, IllegalArgument {
    LOG.info("Waiting for master...");
    InetAddress thriftAddress = cluster.getInstanceMatching(RolePredicates.role(
        HBaseThriftServerClusterActionHandler.ROLE)).getPublicAddress();

    while (true) {
      try {
        getScanner(thriftAddress);
        break;
      } catch (Exception e) {
        try {
          System.out.print(".");
          Thread.sleep(1000);
        } catch (InterruptedException ex) {
          break;
        }
      }
    }
    System.out.println();
    LOG.info("Master reported in. Continuing.");
  }
  
  private void getScanner(InetAddress thriftAddress) throws Exception {
    TTransport transport = new TSocket(thriftAddress.getHostName(),
        HBaseThriftServerClusterActionHandler.PORT);
    transport.open();
    LOG.info("Connected to thrift server.");
    LOG.info("Waiting for .META. table...");
    TProtocol protocol = new TBinaryProtocol(transport, true, true);
    Hbase.Client client = new Hbase.Client(protocol);
    int scannerId = client.scannerOpen(HConstants.META_TABLE_NAME,
        Bytes.toBytes(""), null);
    client.scannerClose(scannerId);
    thriftClient = client;
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

  public Hbase.Client getThriftClient() {
    return thriftClient;
  }
}
