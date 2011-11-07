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

package org.apache.whirr.service;

import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.callables.RunScriptOnNodeAsInitScriptUsingSsh;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.InitBuilder;
import org.junit.Test;

import com.google.common.collect.ListMultimap;
import com.jcraft.jsch.JSchException;

public class DryRunModuleTest {

  public static class Noop2ClusterActionHandler extends
      ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "noop2";
    }

  }

  public static class Noop3ClusterActionHandler extends
      ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "noop3";
    }
  }

  /**
   * Simple test that tests dry run module and at the same time enforces clear
   * separation of script execution phases.
   * 
   * @throws ConfigurationException
   * @throws IOException
   * @throws JSchException
   * @throws InterruptedException
   */
  @Test
  public void testNoInitScriptsAfterConfigurationStartedAndNoConfigScriptsAfterDestroy()
      throws ConfigurationException, JSchException, IOException,
      InterruptedException {

    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.instance-templates",
        "10 noop+noop3,10 noop2+noop,10 noop3+noop2");

    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);
    ClusterController controller = new ClusterController();

    controller.launchCluster(clusterSpec);
    controller.destroyCluster(clusterSpec);

    DryRun dryRun = DryRunModule.getDryRun();
    ListMultimap<NodeMetadata, RunScriptOnNode> perNodeExecutions = dryRun
        .getExecutions();
    List<RunScriptOnNode> totalExecutions = dryRun
        .getTotallyOrderedExecutions();

    // assert that all nodes executed all three phases and in the right order
    for (Entry<NodeMetadata, Collection<RunScriptOnNode>> entry : perNodeExecutions
        .asMap().entrySet()) {
      assertSame("An incorrect number of scripts was executed in the node",
          entry.getValue().size(), 3);
      List<RunScriptOnNode> asList = (List<RunScriptOnNode>) entry.getValue();
      assertTrue("The bootstrap script was executed in the wrong order",
          getScriptName(asList.get(0)).startsWith("setup"));
      assertTrue("The configure script was executed in the wrong order",
          getScriptName(asList.get(1)).startsWith("configure"));
      assertTrue("The destroy script was executed in the wrong order",
          getScriptName(asList.get(2)).startsWith("destroy"));
    }

    // this tests the barrier by making sure that once a configure
    // script is executed no more setup scripts are executed.

    boolean bootPhase = true;
    boolean configPhase = false;
    boolean destroyPhase = false;
    for (RunScriptOnNode script : totalExecutions) {
      if (bootPhase && !configPhase && getScriptName(script).startsWith("configure")) {
        configPhase = true;
        bootPhase = false;
        continue;
      }
      if (configPhase && !destroyPhase && getScriptName(script).startsWith("destroy")) {
        destroyPhase = true;
        configPhase = false;
        continue;
      }
      if (bootPhase) {
        assertTrue(
            "A script other than setup was executed in the bootstrap phase",
            getScriptName(script).startsWith("setup"));
      }
      if (configPhase) {
        assertTrue(
            "A script other than configure was executed in the configure phase",
            getScriptName(script).startsWith("configure"));
      }
      if (destroyPhase) {
        assertTrue(
            "A non-destroy script was executed after the first destroy script. ["
                + getScriptName(script) + "]", getScriptName(script)
                .startsWith("destroy"));
      }
    }
  }

  private String getScriptName(RunScriptOnNode script) {
    return ((InitBuilder) ((RunScriptOnNodeAsInitScriptUsingSsh) script)
        .getStatement()).getInstanceName();
  }

}
