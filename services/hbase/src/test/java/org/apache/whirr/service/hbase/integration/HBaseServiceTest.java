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

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.rest.client.RemoteAdmin;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class HBaseServiceTest {

  private static final String TABLE = "testtable";
  private static final byte [] ROW = Bytes.toBytes("testRow");
  private static final byte [] FAMILY1 = Bytes.toBytes("testFamily1");
  private static final byte [] FAMILY2 = Bytes.toBytes("testFamily2");
  private static final byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static final byte [] VALUE = Bytes.toBytes("testValue");

  private static RemoteAdmin remoteAdmin = null;

  private static HBaseServiceController controller =
    HBaseServiceController.getInstance();

  @BeforeClass
  public static void setUp() throws Exception {
    controller.ensureClusterRunning();
    remoteAdmin = controller.getRemoteAdmin();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    controller.shutdown();
  }

  @Test
  public void test() throws Exception {
    HTableDescriptor td = new HTableDescriptor(TABLE);
    HColumnDescriptor cd = new HColumnDescriptor(FAMILY1);
    td.addFamily(cd);
    cd = new HColumnDescriptor(FAMILY2);
    td.addFamily(cd);
    remoteAdmin.createTable(td);

    RemoteHTable table = controller.getRemoteHTable(TABLE);

    Put put = new Put(ROW);
    put.add(FAMILY1, QUALIFIER, VALUE);
    table.put(put);

    Scan scan = new Scan();
    scan.addColumn(FAMILY2, Bytes.toBytes("bogus"));
    ResultScanner scanner = table.getScanner(scan);
    Result result = scanner.next();
    assertTrue("Expected null result", result == null);
    scanner.close();

    System.out.println("Done.");
  }

}
