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

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.whirr.service.Cluster;
import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.RolePredicates;
import org.apache.whirr.service.Service;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.apache.whirr.service.hbase.HBaseRestServerClusterActionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map.Entry;

public class HBaseServiceController {

  private static final Logger LOG =
    LoggerFactory.getLogger(HBaseServiceController.class);

  private static final HBaseServiceController INSTANCE =
    new HBaseServiceController();

  public static HBaseServiceController getInstance() {
    return INSTANCE;
  }

  private boolean running;
  private ClusterSpec clusterSpec;
  private Service service;
  private HadoopProxy proxy;
  private Cluster cluster;
  private RemoteHTable remoteMetaTable;
  private RemoteAdmin remoteAdmin;
  private Client restClient;

  private HBaseServiceController() {
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
    config.addConfiguration(new PropertiesConfiguration("whirr-hbase-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    service = new Service();

    cluster = service.launchCluster(clusterSpec);
    proxy = new HadoopProxy(clusterSpec, cluster);
    proxy.start();

    Configuration conf = getConfiguration();

    InetAddress restAddress = cluster.getInstanceMatching(RolePredicates.role(
      HBaseRestServerClusterActionHandler.ROLE)).getPublicAddress();
    restClient = new Client(new org.apache.hadoop.hbase.rest.client.Cluster()
      .add(restAddress.getHostName(), HBaseRestServerClusterActionHandler.
        PORT));
    remoteAdmin = new RemoteAdmin(restClient, conf);
    remoteMetaTable = new RemoteHTable(restClient, conf,
      HConstants.META_TABLE_NAME, null);
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

  private void waitForMaster() throws IOException {
    LOG.info("Waiting for master...");
    ResultScanner s = remoteMetaTable.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    LOG.info("Master reported in. Continuing.");
  }

  public synchronized void shutdown() throws IOException, InterruptedException {
    LOG.info("Shutting down cluster...");
    if (proxy != null) {
      proxy.stop();
    }
    if (service != null) {
      service.destroyCluster(clusterSpec);
    }
    running = false;
  }

  public RemoteAdmin getRemoteAdmin() {
    return remoteAdmin;
  }

  public RemoteHTable getRemoteHTable(String tableName) {
    return new RemoteHTable(restClient, getConfiguration(), tableName, null);
  }
}
