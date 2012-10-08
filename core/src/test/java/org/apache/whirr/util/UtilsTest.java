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

package org.apache.whirr.util;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class UtilsTest {

  public static final String EXPECTED_SSH_COMMAND = "\nYou can log into instances using the following ssh commands:\n"
      + "[test-role]: ssh -i /test/key/path -o \"UserKnownHostsFile /dev/null\" -o StrictHostKeyChecking=no test-identity@test-public-ip\n";

  @Test
  public void testPrintAccess() {
    final Instance instance = mock(Instance.class);

    when(instance.getPublicIp()).thenReturn("test-public-ip");
    when(instance.getRoles()).thenReturn(ImmutableSet.of("test-role"));

    Cluster cluster = mock(Cluster.class);

    when(cluster.getInstances()).thenReturn(ImmutableSet.<Cluster.Instance>of(instance));

    ClusterSpec spec = mock(ClusterSpec.class);
    when(spec.getClusterUser()).thenReturn("test-identity");
    when(spec.getPrivateKeyFile()).thenReturn(new File("/test/key/path"));

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(baos);
    Utils.printSSHConnectionDetails(ps, spec, cluster, 20);

    assertEquals("The ssh command did not match", EXPECTED_SSH_COMMAND,
        new String(baos.toByteArray()));
  }

}
