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

package org.apache.whirr.actions.integration;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.ssl.asn1.Strings;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.domain.Statements;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static java.lang.Integer.parseInt;
import static org.apache.commons.lang.StringUtils.deleteWhitespace;
import static org.jclouds.scriptbuilder.domain.Statements.exec;
import static org.junit.Assert.assertTrue;

public class PhaseExecutionBarrierTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(PhaseExecutionBarrierTest.class);

  private static final ConcurrentMap<String, Long> STEP_TIMES = Maps.newConcurrentMap();

  private static long getUnixTime() {
    return System.currentTimeMillis() / 1000L;
  }

  private static void recordTime(String name) {
    STEP_TIMES.put(name, getUnixTime());
  }

  /**
   * A special role implementation that can be used for testing that
   * the install and the configure phase do not overlap
   */
  static public class PhaseBarrierTestActionHandler
    extends ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "phase-barrier-test";
    }

    /**
     * Record timestamps for this phase execution on the remote machine
     */
    @Override
    public void beforeBootstrap(ClusterActionEvent event) {
      addStatement(event, Statements.newStatementList(
        exec("date +%s > /tmp/bootstrap-start"),
        exec("sleep 60"), // 1 minute
        exec("date +%s > /tmp/bootstrap-end")
      ));

      recordTime("before-bootstrap");
    }

    @Override
    public void afterBootstrap(ClusterActionEvent event) {
      recordTime("after-bootstrap");
      /* Note: Scripts are not executed for this step */
    }

    /**
     * Record the timestamp for this phase execution
     */
    @Override
    public void beforeConfigure(ClusterActionEvent event) {
      recordTime("before-configure");
      addStatement(event, exec("date +%s > /tmp/configure-start"));
    }

    @Override
    public void afterConfigure(ClusterActionEvent event) {
      recordTime("after-configure");
      /* Note: Scripts are not executed for this step */
    }

    @Override
    public void beforeDestroy(ClusterActionEvent event) {
      recordTime("before-destroy");
      addStatement(event, exec("data +%s > /tmp/destroy-start"));
    }

    @Override
    public void afterDestroy(ClusterActionEvent event) {
      recordTime("after-destroy");
      /* Note: Scripts are not executed for this step */
    }
  }

  private Configuration getTestConfiguration() throws ConfigurationException {
    return new PropertiesConfiguration("whirr-core-phase-barrier-test.properties");
  }

  private ClusterSpec getTestClusterSpec() throws Exception {
    return ClusterSpec.withTemporaryKeys(getTestConfiguration());
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testNoRemoteExecutionOverlap() throws Exception {
    ClusterSpec spec = getTestClusterSpec();
    ClusterController controller =
      (new ClusterControllerFactory()).create(spec.getServiceName());

    try {
      controller.launchCluster(spec);
      Map<? extends NodeMetadata, ExecResponse> responseMap = controller.runScriptOnNodesMatching(
        spec,
        Predicates.<NodeMetadata>alwaysTrue(),
        exec("cat /tmp/bootstrap-start /tmp/bootstrap-end /tmp/configure-start")
      );
      ExecResponse response = Iterables.get(responseMap.values(), 0);
      LOG.info("Got response: {}", response);

      String[] parts = Strings.split(response.getOutput(), '\n');

      int bootstrapStart = parseInt(deleteWhitespace(parts[0]));
      int bootstrapEnd = parseInt(deleteWhitespace(parts[1]));
      int configureStart = parseInt(deleteWhitespace(parts[2]));

      assertTrue(bootstrapStart < bootstrapEnd);
      assertTrue(bootstrapEnd < configureStart);

    } finally {
      controller.destroyCluster(spec);
    }

    assertNoOverlapOnLocalMachine();
  }

  private void assertNoOverlapOnLocalMachine() {
    String[] listOfSteps = new String[] {"bootstrap", "configure", "destroy"};

    for(int i = 0; i < listOfSteps.length; i += 1) {
      String currentStep = listOfSteps[i];

      assertTrue(
        String.format("Overlap while running '%s'", currentStep),
        STEP_TIMES.get("before-" + currentStep) <= STEP_TIMES.get("after-" + currentStep)
      );

      if (i < (listOfSteps.length - 1)) {
        String nextStep = listOfSteps[i + 1];

        assertTrue(
          String.format("Overlap '%s' and '%s'", currentStep, nextStep),
          STEP_TIMES.get("after-" + currentStep) <= STEP_TIMES.get("before-" + nextStep)
        );
      }
    }
  }

}
