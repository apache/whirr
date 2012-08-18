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

import com.google.common.collect.Lists;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.util.KeyPair;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DestroyInstanceCommandTest extends BaseCommandTest {

  @Test
  public void testInstanceIdMandatory() throws Exception {
    DestroyInstanceCommand command = new DestroyInstanceCommand();
    int rc = command.run(null, out, err, Collections.<String>emptyList());
    assertThat(rc, is(-1));

    String errOutput = errBytes.toString();
    assertThat(errOutput, containsString("--instance-id is a mandatory argument"));
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
