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

import static junit.framework.Assert.assertFalse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.examples.terasort.TeraGen;
import org.apache.hadoop.examples.terasort.TeraSort;
import org.apache.hadoop.examples.terasort.TeraValidate;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.whirr.service.hadoop.integration.HadoopServiceController;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Run the TeraSort benchmark on a cluster.
 */
public class HadoopServiceTeraSortBenchmark {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(HadoopServiceTeraSortBenchmark.class);
  
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
  public void testTeraSort() throws Exception {
    int runs = Integer.parseInt(System.getProperty("terasortRuns", "3"));
    for (int i = 0; i < runs; i++) {
      LOG.info("Starting TeraSort run {} of {}", i + 1, runs);
      runTeraGen();
      runTeraSort();
      runTeraValidate();
          
      FileSystem fs = FileSystem.get(controller.getConfiguration());
      FSDataInputStream in = fs.open(new Path("report/part-00000"));
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      String line = null;
      StringBuilder problems = new StringBuilder();
      boolean failed = false;
      while ((line = reader.readLine()) != null) {
        problems.append(line).append("\n");
        failed = true;
      }
      assertFalse(problems.toString(), failed);
      reader.close();
      
      fs.delete(new Path("input"), true);
      fs.delete(new Path("output"), true);
      fs.delete(new Path("report"), true);
      LOG.info("Completed TeraSort run {} of {}", i + 1, runs);
    }
  }

  private void runTeraGen() throws IOException {
    int numTaskTrackers = controller.getCluster().getInstances().size() - 1;
    long bytesPerNode =
      Long.parseLong(System.getProperty("terasortBytesPerNode", "1000000000"));
    long rows = numTaskTrackers * (bytesPerNode / 100);
    StopWatch stopWatch = new StopWatch();
    TeraGen teraGen = new TeraGen();
    teraGen.setConf(controller.getJobConf());
    LOG.info("Starting TeraGen with {} tasktrackers, {} bytes per node, {} rows",
        new Object[] { numTaskTrackers, bytesPerNode, rows});
    stopWatch.start();
    teraGen.run(new String[] { "" + rows, "input" });
    stopWatch.stop();
    LOG.info("TeraGen took {} ms", stopWatch.getTime());
  }

  private void runTeraSort() throws Exception {
    StopWatch stopWatch = new StopWatch();
    TeraSort teraSort = new TeraSort();
    teraSort.setConf(controller.getJobConf());
    LOG.info("Starting TeraSort");
    stopWatch.start();
    teraSort.run(new String[] { "input", "output" });
    stopWatch.stop();
    LOG.info("TeraSort took {} ms", stopWatch.getTime());
  }

  private void runTeraValidate() throws Exception {
    StopWatch stopWatch = new StopWatch();
    TeraValidate teraValidate = new TeraValidate();
    teraValidate.setConf(controller.getJobConf());
    LOG.info("Starting TeraValidate");
    stopWatch.start();
    teraValidate.run(new String[] { "output", "report" });
    stopWatch.stop();
    LOG.info("TeraValidate took {} ms", stopWatch.getTime());
  }
}
