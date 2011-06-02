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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Map;

import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.util.KeyPair;
import org.junit.Before;
import org.junit.Test;

public class DestroyInstanceCommandTest {

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
  public void testInstanceIdMandatory() throws Exception {
    DestroyInstanceCommand command = new DestroyInstanceCommand();
    int rc = command.run(null, out, err, Collections.<String>emptyList());
    assertThat(rc, is(-1));

    String errOutput = errBytes.toString();
    assertThat(errOutput, containsString("You need to specify " +
        "an instance ID."));
    assertThat(errOutput, containsString("Usage: whirr destroy-instance" +
        " --instance-id <region/ID> [OPTIONS]"));
  }

  @Test
  public void testDestroyInstanceById() throws Exception {
    ClusterControllerFactory factory = mock(ClusterControllerFactory.class);
    ClusterController controller = mock(ClusterController.class);
    when(factory.create((String) any())).thenReturn(controller);

    DestroyInstanceCommand command = new DestroyInstanceCommand(factory);
    Map<String, File> keys = KeyPair.generateTemporaryFiles();

    int rc = command.run(null, out, null, Lists.newArrayList(
        "--instance-templates", "1 noop",
        "--instance-id", "region/instanceid",
        "--service-name", "test-service",
        "--cluster-name", "test-cluster",
        "--provider", "rackspace",
        "--identity", "myusername", "--credential", "mypassword",
        "--private-key-file", keys.get("private").getAbsolutePath(),
        "--version", "version-string"
        ));
    assertThat(rc, is(0));

    verify(controller).destroyInstance((ClusterSpec) any(),
        eq("region/instanceid"));
  }
}
