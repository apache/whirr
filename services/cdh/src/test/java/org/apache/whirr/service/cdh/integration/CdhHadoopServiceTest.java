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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;
import java.util.Map.Entry;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.failNotEquals;

public class CdhHadoopServiceTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(CdhHadoopServiceTest.class);

  private final static Predicate<NodeMetadata> ALL = Predicates.alwaysTrue();

  protected static ClusterSpec clusterSpec;
  protected static ClusterController controller;
  protected static HadoopProxy proxy;
  protected static Cluster cluster;

  protected static String getPropertiesFilename() {
    return "whirr-hadoop-test.properties";
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration(getPropertiesFilename()));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    
    cluster = controller.launchCluster(clusterSpec);
    proxy = new HadoopProxy(clusterSpec, cluster);
    proxy.start();
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (proxy != null) {
      proxy.stop();
    }
    controller.destroyCluster(clusterSpec);
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testVersion() throws Exception {
    Statement checkVersion = Statements.exec("ls /etc/alternatives/hadoop-lib");
    Map<? extends NodeMetadata, ExecResponse> responses =
       controller.runScriptOnNodesMatching(clusterSpec, ALL, checkVersion);

    printResponses(checkVersion, responses);
    assertResponsesContain(responses, checkVersion, "cdh3u2");
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testJobExecution() throws Exception {
    Configuration conf = getConfiguration();
    
    JobConf job = new JobConf(conf, CdhHadoopServiceTest.class);
    JobClient client = new JobClient(job);
    waitForTaskTrackers(client);

    checkHadoop(conf, client, job);
  }
  
  protected void checkHadoop(Configuration conf, JobClient client, JobConf job) throws Exception {
    FileSystem fs = FileSystem.get(conf);
    
    OutputStream os = fs.create(new Path("input"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();
    
    job.setMapperClass(TokenCountMapper.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.setInputPaths(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));
    
    JobClient.runJob(job);

    FSDataInputStream in = fs.open(new Path("output/part-00000"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    assertEquals("a\t1", reader.readLine());
    assertEquals("b\t1", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }
  
  private Configuration getConfiguration() {
    Configuration conf = new Configuration();
    for (Entry<Object, Object> entry : cluster.getConfiguration().entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }
  
  private static void waitForTaskTrackers(JobClient client) throws IOException {
    while (true) {
      ClusterStatus clusterStatus = client.getClusterStatus();
      int taskTrackerCount = clusterStatus.getTaskTrackers();
      if (taskTrackerCount > 0) {
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

  private static void assertResponsesContain(
      Map<? extends NodeMetadata, ExecResponse> responses, Statement statement,
      String text) {
    for (Map.Entry<? extends NodeMetadata, ExecResponse> entry : responses
        .entrySet()) {
      if (!entry.getValue().getOutput().contains(text)) {
        failNotEquals("Node: " + entry.getKey().getId()
            + " failed to execute the command: " + statement
            + " as could not find expected text", text, entry.getValue());
      }
    }
  }

  public static void printResponses(Statement statement,
      Map<? extends NodeMetadata, ExecResponse> responses) {
    LOG.info("Responses for Statement: " + statement);
    for (Map.Entry<? extends NodeMetadata, ExecResponse> entry : responses
        .entrySet()) {
      LOG.info("Node[" + entry.getKey().getId() + "]: " + entry.getValue());
    }
  }

}
