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

package org.apache.whirr.command;

import joptsimple.OptionSet;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.util.KeyPair;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class AbstractClusterCommandTest {

  @Test
  public void testOverrides() throws Exception {
    AbstractClusterCommand clusterCommand = new AbstractClusterCommand("name",
        "description", new ClusterControllerFactory()) {
      @Override
      public int run(InputStream in, PrintStream out, PrintStream err,
          List<String> args) throws Exception {
        return 0;
      }
    };

    Map<String, File> keys = KeyPair.generateTemporaryFiles();
    OptionSet optionSet = clusterCommand.parser.parse(
        "--service-name", "overridden-test-service",
        "--config", "whirr-override-test.properties",
        "--private-key-file", keys.get("private").getAbsolutePath()
    );
    ClusterSpec clusterSpec = clusterCommand.getClusterSpec(optionSet);
    assertThat(clusterSpec.getServiceName(), is("overridden-test-service"));
    assertThat(clusterSpec.getClusterName(), is("test-cluster"));
  }

  /**
   * Ensure that an invalid service name uses the default (after logging a
   * warning).
   */
  @Test
  public void testCreateServerWithInvalidClusterControllerName() throws Exception {
    AbstractClusterCommand clusterCommand = new AbstractClusterCommand("name",
        "description", new ClusterControllerFactory()) {
      @Override
      public int run(InputStream in, PrintStream out, PrintStream err,
          List<String> args) throws Exception {
        return 0;
      }
    };

    // following should not fail
    clusterCommand.createClusterController("bar");
  }
}
