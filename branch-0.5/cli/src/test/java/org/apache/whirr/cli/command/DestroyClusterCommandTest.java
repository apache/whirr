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

package org.apache.whirr.cli.command;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.util.KeyPair;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.junit.internal.matchers.StringContains;

public class DestroyClusterCommandTest {

  private ByteArrayOutputStream outBytes;
  private PrintStream out;
  private ByteArrayOutputStream errBytes;
  private PrintStream err;

  @Before
  public void setUp() {
    outBytes = new ByteArrayOutputStream();
    out = new PrintStream(outBytes);
    errBytes = new ByteArrayOutputStream();
    err = new PrintStream(errBytes);
  }
  
  @Test
  public void testInsufficientOptions() throws Exception {
    DestroyClusterCommand command = new DestroyClusterCommand();
    int rc = command.run(null, null, err, Collections.<String>emptyList());
    assertThat(rc, is(-1));
    assertThat(errBytes.toString(), containsUsageString());
  }
  
  private Matcher<String> containsUsageString() {
    return StringContains.containsString("Usage: whirr destroy-cluster [OPTIONS]");
  }
  
  @Test
  public void testAllOptions() throws Exception {
    
    ClusterControllerFactory factory = mock(ClusterControllerFactory.class);
    ClusterController controller = mock(ClusterController.class);
    when(factory.create((String) any())).thenReturn(controller);
    
    DestroyClusterCommand command = new DestroyClusterCommand(factory);
    Map<String, File> keys = KeyPair.generateTemporaryFiles();
    
    int rc = command.run(null, out, null, Lists.newArrayList(
        "--instance-templates", "1 noop",
        "--service-name", "test-service",
        "--cluster-name", "test-cluster",
        "--provider", "rackspace",
        "--identity", "myusername", "--credential", "mypassword",
        "--private-key-file", keys.get("private").getAbsolutePath(),
        "--version", "version-string"
        ));
    
    assertThat(rc, is(0));

    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.version", "version-string");
    conf.addProperty("whirr.instance-templates", "1 noop");

    ClusterSpec expectedClusterSpec = ClusterSpec.withTemporaryKeys(conf);
    expectedClusterSpec.setServiceName("test-service");
    expectedClusterSpec.setProvider("rackspace");
    expectedClusterSpec.setIdentity("myusername");
    expectedClusterSpec.setCredential("mypassword");
    expectedClusterSpec.setClusterName("test-cluster");
    expectedClusterSpec.setPrivateKey(keys.get("private"));
    expectedClusterSpec.setPublicKey(keys.get("public"));
    
    verify(factory).create("test-service");
    
    verify(controller).destroyCluster(expectedClusterSpec);
    
  }
}
