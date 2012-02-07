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

import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.ClusterStatus;
import org.apache.hama.examples.PiEstimator;
import org.apache.hama.examples.PiEstimator.MyEstimator;
import org.apache.whirr.TestConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HamaServiceTest {
  private static HamaServiceController controller = HamaServiceController
      .getInstance();
  private static String masterTask = "master.task.";
  
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
    HamaConfiguration conf = controller.getConfiguration();

    BSPJobClient jobClient = new BSPJobClient(conf);
    ClusterStatus cluster = jobClient.getClusterStatus(true);
    assertNotNull(cluster);
    assertTrue(cluster.getGroomServers() > 0);

    BSPJob bsp = new BSPJob(conf, PiEstimator.class);
    // Set the job name
    bsp.setJobName("Pi Estimation Example");
    bsp.setBspClass(MyEstimator.class);
    bsp.setNumBspTask(cluster.getGroomServers());

    for (String peerName : cluster.getActiveGroomNames().values()) {
      conf.set(masterTask, peerName);
      break;
    }

    if (bsp.waitForCompletion(true)) {
      assertEquals(jobClient.getAllJobs().length, 1);
    }
  }

}
