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

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.thrift.generated.ColumnDescriptor;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.apache.hadoop.hbase.thrift.generated.Mutation;
import org.apache.hadoop.hbase.thrift.generated.TRowResult;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.hbase.integration.HBaseServiceController;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CdhHBaseServiceTest {

  private static final byte[] FIRST = Bytes.toBytes("");
  private static final byte[] TABLE = Bytes.toBytes("testtable");
  private static final byte[] ROW = Bytes.toBytes("testRow");
  private static final byte[] FAMILY1 = Bytes.toBytes("testFamily1");
  private static final byte[] FAMILY2 = Bytes.toBytes("testFamily2");
  private static final byte[] COLUMN = Bytes.toBytes("testFamily1:testColumn");
  private static final byte[] VALUE = Bytes.toBytes("testValue");

  private static HBaseServiceController controller =
    HBaseServiceController.getInstance("whirr-hbase-test.properties");

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
    ArrayList<ColumnDescriptor> columns = new ArrayList<ColumnDescriptor>();
    ColumnDescriptor cd = new ColumnDescriptor();
    cd.name = FAMILY1;
    columns.add(cd);
    cd = new ColumnDescriptor();
    cd.name = FAMILY2;
    columns.add(cd);

    Hbase.Client client = controller.getThriftClient();
    client.createTable(TABLE, columns);

    ArrayList<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(new Mutation(false, COLUMN, VALUE));
    client.mutateRow(TABLE, ROW, mutations);
    
    int scan1 = client.scannerOpen(TABLE, FIRST, Lists.newArrayList(FAMILY1));
    List<TRowResult> rows = client.scannerGet(scan1);
    assertThat(rows.size(), is(1));
    assertThat(Bytes.toString(rows.get(0).getRow()), is("testRow"));
    assertTrue("No more rows", client.scannerGet(scan1).isEmpty());
    client.scannerClose(scan1);

    int scan2 = client.scannerOpen(TABLE, FIRST, Lists.newArrayList(FAMILY2));
    assertTrue("No more rows", client.scannerGet(scan2).isEmpty());
    client.scannerClose(scan2);
  }

}
