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

package org.apache.whirr.examples;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.FSConstants;
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
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.hadoop.HadoopProxy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

/**
 * This class shows how you should use Whirr to start a Hadoop
 * cluster and submit a mapreduce job.
 *
 * The code was extracted from the Hadoop integration tests.
 */
public class HadoopClusterExample extends Example {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopClusterExample.class);

  @Override
  public String getName() {
    return "hadoop-cluster";
  }

  @Override
  public int main(String[] args) throws Exception {

    /**
     * Assumption: Cloud credentials are available in the current environment
     *
     * export AWS_ACCESS_KEY_ID=...
     * export AWS_SECRET_ACCESS_KEY=...
     *
     * You can find the credentials for EC2 by following this steps:
     * 1. Go to http://aws-portal.amazon.com/gp/aws/developer/account/index.html?action=access-key
     * 2. Log in, if prompted
     * 3. Find your Access Key ID and Secret Access Key in the "Access Credentials"
     * section, under the "Access Keys" tab. You will have to click "Show" to see
     * the text of your secret access key.
     *
     */

    if (!System.getenv().containsKey("AWS_ACCESS_KEY_ID")) {
      LOG.error("AWS_ACCESS_KEY_ID is undefined in the current environment");
      return -1;
    }
    if (!System.getenv().containsKey("AWS_SECRET_ACCESS_KEY")) {
      LOG.error("AWS_SECRET_ACCESS_KEY is undefined in the current environment");
      return -2;
    }

    /**
     * Start by loading cluster configuration file and creating a ClusterSpec object
     *
     * You can find the file in the resources folder.
     */
    ClusterSpec spec = new ClusterSpec(
      new PropertiesConfiguration("whirr-hadoop-example.properties"));

    /**
     * Create an instance of the generic cluster controller
     */
    ClusterControllerFactory factory = new ClusterControllerFactory();
    ClusterController controller = factory.create(spec.getServiceName());

    /**
     * Start the cluster as defined in the configuration file
     */
    HadoopProxy proxy = null;
    try {
      LOG.info("Starting cluster {}", spec.getClusterName());
      Cluster cluster = controller.launchCluster(spec);

      LOG.info("Starting local SOCKS proxy");
      proxy = new HadoopProxy(spec, cluster);
      proxy.start();

      /**
       * Obtain a Hadoop configuration object and wait for services to start
       */
      Configuration config = getHadoopConfiguration(cluster);
      JobConf job = new JobConf(config, HadoopClusterExample.class);
      JobClient client = new JobClient(job);

      waitToExitSafeMode(client);
      waitForTaskTrackers(client);

      /**
       * Run a simple job to show that the cluster is available for work.
       */

      runWordCountingJob(config);

    } finally {
      /**
       * Stop the proxy and terminate all the cluster instances.
       */
      if (proxy != null) {
        proxy.stop();
      }
      controller.destroyCluster(spec);
      return 0;
    }
  }

  private void runWordCountingJob(Configuration config) throws IOException {
    JobConf job = new JobConf(config, HadoopClusterExample.class);

    FileSystem fs = FileSystem.get(config);

    OutputStream os = fs.create(new Path("input"));
    Writer wr = new OutputStreamWriter(os);
    wr.write("b a\n");
    wr.close();

    LOG.info("Wrote a file containing 'b a\\n'");

    job.setMapperClass(TokenCountMapper.class);
    job.setReducerClass(LongSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    FileInputFormat.setInputPaths(job, new Path("input"));
    FileOutputFormat.setOutputPath(job, new Path("output"));

    JobClient.runJob(job);

    FSDataInputStream in = fs.open(new Path("output/part-00000"));
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));

    String line = reader.readLine();
    int count = 0;
    while (line != null) {
      LOG.info("Line {}: {}", count, line);
      count += 1;
      line = reader.readLine();
    }
    reader.close();
  }

  private Configuration getHadoopConfiguration(Cluster cluster) {
    Configuration conf = new Configuration();
    for (Map.Entry<Object, Object> entry : cluster.getConfiguration().entrySet()) {
      conf.set(entry.getKey().toString(), entry.getValue().toString());
    }
    return conf;
  }

  private void waitToExitSafeMode(JobClient client) throws IOException {
    LOG.info("Waiting to exit safe mode...");
    FileSystem fs = client.getFs();
    DistributedFileSystem dfs = (DistributedFileSystem) fs;
    boolean inSafeMode = true;
    while (inSafeMode) {
      inSafeMode = dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_GET);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
    LOG.info("Exited safe mode");
  }

  private void waitForTaskTrackers(JobClient client) throws IOException {
    LOG.info("Waiting for tasktrackers...");
    while (true) {
      ClusterStatus clusterStatus = client.getClusterStatus();
      int taskTrackerCount = clusterStatus.getTaskTrackers();
      if (taskTrackerCount > 0) {
        LOG.info("{} tasktrackers reported in. Continuing.", taskTrackerCount);
        break;
      }
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        break;
      }
    }
  }

}
