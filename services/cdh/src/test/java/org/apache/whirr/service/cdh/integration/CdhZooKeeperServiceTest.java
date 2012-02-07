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

package org.apache.whirr.service.cdh.integration;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import static junit.framework.Assert.assertEquals;

public class CdhZooKeeperServiceTest {
  
  private ClusterSpec clusterSpec;
  private Cluster cluster;
  private ClusterController controller;
  private String hosts;
  
  @Before
  public void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-zookeeper-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    
    cluster = controller.launchCluster(clusterSpec);
    hosts = ZooKeeperCluster.getHosts(cluster);
  }

  @Test (timeout = TestConstants.ITEST_TIMEOUT)
  public void test() throws Exception {
    class ConnectionWatcher implements Watcher {

      private ZooKeeper zk;
      private CountDownLatch latch = new CountDownLatch(1);
      
      public void connect(String hosts) throws IOException, InterruptedException {
        zk = new ZooKeeper(hosts, 5000, this);
        latch.await();
      }
      
      public ZooKeeper getZooKeeper() {
        return zk;
      }
      
      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == KeeperState.SyncConnected) {
          latch.countDown();
        }
      }
      
      public void close() throws InterruptedException {
        if (zk != null) {
          zk.close();
        }
      }
      
    }
    
    String path = "/data";
    String data = "Hello";
    ConnectionWatcher watcher = new ConnectionWatcher();
    watcher.connect(hosts);
    watcher.getZooKeeper().create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    watcher.close();
    
    watcher = new ConnectionWatcher();
    watcher.connect(hosts);
    byte[] actualData = watcher.getZooKeeper().getData(path, false, null);
    assertEquals(data, new String(actualData));
    watcher.close();
  }
  
  @After
  public void tearDown() throws IOException, InterruptedException {
    controller.destroyCluster(clusterSpec);
  }
  
}
