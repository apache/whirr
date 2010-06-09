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

package org.apache.whirr.service.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;
import static junit.framework.Assert.assertEquals;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.service.ServiceSpec;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperServiceTest {
  
  private String clusterName = "zkclustertest";
  
  private ZooKeeperService service;
  private ZooKeeperCluster cluster;
  
  @Before
  public void setUp() throws IOException {
    String secretKeyFile;
    try {
       secretKeyFile = checkNotNull(System.getProperty("whirr.test.ssh.keyfile"));
    } catch (NullPointerException e) {
       secretKeyFile = System.getProperty("user.home") + "/.ssh/id_rsa";
    }
    ServiceSpec serviceSpec = new ServiceSpec();
    serviceSpec.setProvider(checkNotNull(System.getProperty("whirr.test.provider", "ec2")));
    serviceSpec.setAccount(checkNotNull(System.getProperty("whirr.test.user")));
    serviceSpec.setKey(checkNotNull(System.getProperty("whirr.test.key")));
    serviceSpec.setSecretKeyFile(secretKeyFile);
    serviceSpec.setClusterName(clusterName);
    service = new ZooKeeperService(serviceSpec);
    
    ClusterSpec clusterSpec = new ClusterSpec(
	new InstanceTemplate(2, ZooKeeperService.ZOOKEEPER_ROLE));
    cluster = service.launchCluster(clusterSpec);
    System.out.println(cluster.getHosts());
  }
  
  @Test
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
    watcher.connect(cluster.getHosts());
    watcher.getZooKeeper().create(path, data.getBytes(), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);
    watcher.close();
    
    watcher = new ConnectionWatcher();
    watcher.connect(cluster.getHosts());
    byte[] actualData = watcher.getZooKeeper().getData(path, false, null);
    assertEquals(data, new String(actualData));
    watcher.close();
  }
  
  @After
  public void tearDown() throws IOException {
    service.destroyCluster();
  }
  
}
