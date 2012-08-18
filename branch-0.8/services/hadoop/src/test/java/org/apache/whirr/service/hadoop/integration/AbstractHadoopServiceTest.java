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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.mapred.lib.TokenCountMapper;
import org.apache.whirr.TestConstants;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Subclasses should implement @BeforeClass static void setUp()
 * to set the controller.
 */
public abstract class AbstractHadoopServiceTest {

  protected static HadoopServiceController controller;
  
  @AfterClass
  public static void tearDown() throws Exception {
    controller.shutdown();
  }

  public AbstractHadoopServiceTest() {
    super();
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void test() throws Exception {
    Configuration conf = controller.getConfiguration();
    JobConf job = new JobConf(conf, HadoopServiceTest.class);
  
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

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testExistsTemporaryFolderAndHiveWarehouse() throws Exception {
    Configuration conf = controller.getConfiguration();
    FileSystem fs = FileSystem.get(conf);
  
    assertThat(fs.exists(new Path("/tmp")), is(true));
    assertThat(fs.exists(new Path("/user/hive/warehouse")), is(true));
  }

}
