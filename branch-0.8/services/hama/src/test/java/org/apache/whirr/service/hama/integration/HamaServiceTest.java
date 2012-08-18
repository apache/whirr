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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.NullInputFormat;
import org.apache.hama.bsp.RunningJob;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.examples.PiEstimator.MyEstimator;
import org.apache.whirr.TestConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HamaServiceTest {
  private static final Logger LOG = LoggerFactory
      .getLogger(HamaServiceTest.class);

  private static HamaServiceController controller = HamaServiceController
      .getInstance();

  private final static Path TMP_OUTPUT = new Path("/pi-"
      + System.currentTimeMillis());

  @BeforeClass
  public static void setUp() throws Exception {
    controller.ensureClusterRunning();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    controller.shutdown();
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void test() throws Exception {
    HamaConfiguration jobConf = controller.getConfiguration();
    jobConf.set("hadoop.rpc.socket.factory.class.default",
        "org.apache.hadoop.net.StandardSocketFactory");

    BSPJob bsp = new BSPJob(jobConf, new BSPJobID());
    LOG.info("Job conf: "
        + bsp.getConf().get("hadoop.rpc.socket.factory.class.default") + ", "
        + bsp.getJobID().toString());

    bsp.setJarByClass(MyEstimator.class);
    bsp.setBspClass(MyEstimator.class);
    bsp.setInputFormat(NullInputFormat.class);
    bsp.setOutputKeyClass(Text.class);
    bsp.setOutputValueClass(DoubleWritable.class);
    bsp.setOutputFormat(TextOutputFormat.class);
    bsp.set("bsp.working.dir", "/tmp");
    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT);

    LOG.info("Client configuration start ..");

    HamaConfiguration clientConf = controller.getConfiguration();
    BSPJobClient jobClient = new BSPJobClient(clientConf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);
    assertNotNull(cluster);
    assertTrue(cluster.getGroomServers() > 0);
    assertTrue(cluster.getMaxTasks() > 1);
    bsp.setNumBspTask(cluster.getMaxTasks());

    LOG.info("Client conf: "
        + clientConf.get("hadoop.rpc.socket.factory.class.default"));

    RunningJob rJob = jobClient.submitJob(bsp);
    rJob.waitForCompletion();
    LOG.info("finished");
  }

}
