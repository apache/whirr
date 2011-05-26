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
package org.apache.whirr.service.hadoop.integration.benchmark;

import org.apache.hadoop.fs.TestDFSIO;
import org.apache.hadoop.mapred.JobConf;
import org.apache.whirr.service.hadoop.integration.HadoopServiceController;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopServiceTestDFSIOBenchmark {
  private static final Logger LOG =
    LoggerFactory.getLogger(HadoopServiceTestDFSIOBenchmark.class);

  private static boolean shutdownClusterOnTearDown;
  private static HadoopServiceController controller =
    HadoopServiceController.getInstance();

  
  @BeforeClass
  public static void setUp() throws Exception {
    shutdownClusterOnTearDown = controller.ensureClusterRunning();
  }
  
  @AfterClass
  public static void tearDown() throws Exception {
    if (shutdownClusterOnTearDown) {
      controller.shutdown();
    }
  }
  
  @Test
  public void test() throws Exception {
    // We use TestDFSIO's Tool interface so we can pass the configuration,
    // but this is only available after Hadoop 0.20.2
    // (https://issues.apache.org/jira/browse/MAPREDUCE-1832)
    // so we use a local copy of TestDFSIO with this patch
    int runs = Integer.parseInt(System.getProperty("testDFSIORuns", "3"));
    for (int i = 0; i < runs; i++) {
      LOG.info("Starting TestDFSIO run {} of {}", i + 1, runs);
      TestDFSIO testDFSIO = new TestDFSIO();
      JobConf jobConf = controller.getJobConf();
      jobConf.set("test.build.data", "/user/root/benchmark/TestDFSIO");
      testDFSIO.setConf(jobConf);
      testDFSIO.run("-write -nrFiles 10 -fileSize 1000".split(" "));
      testDFSIO.run("-read -nrFiles 10 -fileSize 1000".split(" "));
      testDFSIO.run(new String[] { "-clean" });
      LOG.info("Completed TestDFSIO run {} of {}", i + 1, runs);
    }
  }
}
