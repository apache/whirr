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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.events.StatementOnNode;
import org.jclouds.scriptbuilder.InitScript;
import org.jclouds.scriptbuilder.domain.Statement;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.jcraft.jsch.JSchException;

public class DryRunModuleTest {

  public static class Noop2ClusterActionHandler extends
      ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "noop2";
    }

    @Override
    public void beforeConfigure(ClusterActionEvent event) {
      addStatement(event, exec("echo noop2-configure"));
    }

    @Override
    public void beforeStart(ClusterActionEvent event) {
      addStatement(event, exec("echo noop2-start"));
    }

    @Override
    public void beforeStop(ClusterActionEvent event) {
      addStatement(event, exec("echo noop2-stop"));
    }

    @Override
    public void beforeCleanup(ClusterActionEvent event) {
      addStatement(event, exec("echo noop2-cleanup"));
    }

    @Override
    public void beforeDestroy(ClusterActionEvent event) {
      addStatement(event, exec("echo noop2-destroy"));
    }
  }

  public static class Noop3ClusterActionHandler extends
      ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "noop3";
    }

    @Override
    public void beforeConfigure(ClusterActionEvent event) {
      addStatement(event, exec("echo noop3-configure"));
    }

    @Override
    public void beforeStart(ClusterActionEvent event) {
      addStatement(event, exec("echo noop3-start"));
    }

    @Override
    public void beforeStop(ClusterActionEvent event) {
      addStatement(event, exec("echo noop3-stop"));
    }

    @Override
    public void beforeCleanup(ClusterActionEvent event) {
      addStatement(event, exec("echo noop3-cleanup"));
    }

    @Override
    public void beforeDestroy(ClusterActionEvent event) {
      addStatement(event, exec("echo noop3-destroy"));
    }
  }

  @Test
  public void testExecuteOnlyBootstrapForNoop() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.instance-templates", "1 noop");
    config.setProperty("whirr.state-store", "memory");

    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);
    ClusterController controller = new ClusterController();

    DryRun dryRun = getDryRunInControllerForCluster(controller, clusterSpec);
    dryRun.reset();
    
    controller.launchCluster(clusterSpec);
    controller.destroyCluster(clusterSpec);

    ListMultimap<NodeMetadata, Statement> perNodeExecutions = dryRun.getExecutions();

    for (Entry<NodeMetadata, Collection<Statement>> entry : perNodeExecutions
        .asMap().entrySet()) {
      assertSame("An incorrect number of scripts was executed in the node " + entry,
          entry.getValue().size(), 1);
    }
  }

  /**
   * Simple test that tests dry run module and at the same time enforces clear
   * separation of script execution phases.
   */
  @Test
  public void testNoInitScriptsAfterConfigurationStartedAndNoConfigScriptsAfterDestroy()
      throws ConfigurationException, JSchException, IOException, InterruptedException {

    final List<String> expectedExecutionOrder = ImmutableList.of("bootstrap", "configure", "start", "destroy");

    CompositeConfiguration config = new CompositeConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.instance-templates", "10 noop+noop3,10 noop2+noop,10 noop3+noop2");
    config.setProperty("whirr.state-store", "memory");

    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);
    ClusterController controller = new ClusterController();
    
    DryRun dryRun = getDryRunInControllerForCluster(controller, clusterSpec);
    dryRun.reset();
    
    controller.launchCluster(clusterSpec);
    controller.destroyCluster(clusterSpec);

    
    ListMultimap<NodeMetadata, Statement> perNodeExecutions = dryRun.getExecutions();
    List<StatementOnNode> totalExecutions = dryRun.getTotallyOrderedExecutions();

    // Assert that all nodes executed all three phases and in the right order

    for (Entry<NodeMetadata, Collection<Statement>> entry : perNodeExecutions
        .asMap().entrySet()) {
      assertSame("An incorrect number of scripts was executed in the node: " + entry.getValue(),
          entry.getValue().size(), expectedExecutionOrder.size());
      List<Statement> asList = Lists.newArrayList(entry.getValue());

      int count = 0;
      for(String phase : expectedExecutionOrder) {
        String scriptName = getScriptName(asList.get(count));
        assertTrue("The '" + phase + "' script was executed in the wrong order, found: " + scriptName,
              scriptName.startsWith(phase));
        count += 1;
      }
    }

    // This tests the barrier by making sure that once a configure
    // script is executed no more setup scripts are executed

    Stack<String> executedPhases = new Stack<String>();
    for (StatementOnNode script : totalExecutions) {
      String[] parts = getScriptName(script.getStatement()).split("-");
      if ((!executedPhases.empty() && !executedPhases.peek().equals(parts[0])) || executedPhases.empty()) {
        executedPhases.push(parts[0]);
      }
    }

    // Assert that all scripts executed in the right order with no overlaps

    assertEquals(expectedExecutionOrder.size(), executedPhases.size());
    for (String phaseName : Lists.reverse(expectedExecutionOrder)) {
      assertEquals(executedPhases.pop(), phaseName);
    }
  }

public DryRun getDryRunInControllerForCluster(ClusterController controller, ClusterSpec clusterSpec) {
   DryRun dryRun = controller.getCompute().apply(clusterSpec).utils().injector().getInstance(DryRun.class);
   return dryRun;
}
  
  private String getScriptName(Statement script) {
      return InitScript.class.cast(script).getInstanceName();
  }

}
