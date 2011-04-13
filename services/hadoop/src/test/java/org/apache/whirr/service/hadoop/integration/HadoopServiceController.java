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

package org.apache.whirr.service.hadoop.integration;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopServiceController {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(HadoopServiceController.class);

  private static final HadoopServiceController INSTANCE =
    new HadoopServiceController();
  
  public static HadoopServiceController getInstance() {
    return INSTANCE;
  }
  
  private boolean running;
  private ClusterSpec clusterSpec;
  private ClusterController controller;
  private HadoopProxy proxy;
  private Cluster cluster;
  
  private HadoopServiceController() {
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
    config.addConfiguration(new PropertiesConfiguration("whirr-hadoop-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    
    cluster = controller.launchCluster(clusterSpec);
    proxy = new HadoopProxy(clusterSpec, cluster);
    proxy.start();
    
    Configuration conf = getConfiguration();
    JobConf job = new JobConf(conf, HadoopServiceTest.class);
    JobClient client = new JobClient(job);
    waitToExitSafeMode(client);
    waitForTaskTrackers(client);
    running = true;
  }
  
  public Cluster getCluster() {
    return cluster;
  }
  
  public Configuration getConfiguration() {
    Configuration conf = new Configuration();
    for (Entry<Object, Object> entry : cluster.getConfiguration().entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }
  
  public JobConf getJobConf() {
    return new JobConf(getConfiguration());
  }
  
  private static void waitToExitSafeMode(JobClient client) throws IOException {
    LOG.info("Waiting to exit safe mode...");
    FileSystem fs = client.getFs();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    boolean inSafeMode = true;
    while (inSafeMode) {
      inSafeMode = dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_GET);
      try {
        System.out.print(".");
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
    LOG.info("Exited safe mode");
  }
  
  private static void waitForTaskTrackers(JobClient client) throws IOException {
    LOG.info("Waiting for tasktrackers...");
    while (true) {
      ClusterStatus clusterStatus = client.getClusterStatus();
      int taskTrackerCount = clusterStatus.getTaskTrackers();
      if (taskTrackerCount > 0) {
        LOG.info("{} tasktrackers reported in. Continuing.", taskTrackerCount);
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
  
  public synchronized void shutdown() throws IOException, InterruptedException {
    LOG.info("Shutting down cluster...");
    if (proxy != null) {
      proxy.stop();
    }
    controller.destroyCluster(clusterSpec);
    running = false;
  }

}
