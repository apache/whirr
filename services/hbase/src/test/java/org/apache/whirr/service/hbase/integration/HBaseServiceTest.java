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

package org.apache.whirr.service.hbase.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HBaseServiceTest {

  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");

  private static HBaseServiceController controller =
    HBaseServiceController.getInstance();

  @BeforeClass
  public static void setUp() throws Exception {
    controller.ensureClusterRunning();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    controller.shutdown();
  }

  @Test
  public void test() throws Exception {
    Configuration conf = controller.getConfiguration();
    HBaseTestingUtility testUtil = new HBaseTestingUtility(conf);
    byte [] TABLE = Bytes.toBytes("testtable");
    HTable ht = testUtil.createTable(TABLE, FAMILY);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, VALUE);
    ht.put(put);
    Scan scan = new Scan();
    scan.addColumn(FAMILY, TABLE);
    ResultScanner scanner = ht.getScanner(scan);
    Result result = scanner.next();
    assertTrue("Expected null result", result == null);
    scanner.close();
    System.out.println("Done.");
  }

}
