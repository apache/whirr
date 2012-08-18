/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.whirr.service.mahout.integration;

import com.google.common.base.Predicate;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static com.google.common.base.Predicates.alwaysTrue;
import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Sets.newHashSet;
import static junit.framework.Assert.failNotEquals;
import static org.apache.whirr.RolePredicates.anyRoleIn;
import static org.apache.whirr.service.mahout.MahoutClientClusterActionHandler.MAHOUT_CLIENT_ROLE;
import static org.jclouds.compute.predicates.NodePredicates.withIds;

/**
 * Install the mahout binary distribution.
 */
public class MahoutServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(MahoutServiceTest.class);

  private static ClusterSpec clusterSpec;
  private static ClusterController controller;

  @BeforeClass
  public static void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-mahout-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    controller.launchCluster(clusterSpec);
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (controller != null) {
      controller.destroyCluster(clusterSpec);
    }
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testBinMahout() throws Exception {
    Statement binMahout = Statements.exec("source /etc/profile; $MAHOUT_HOME/bin/mahout");

    Cluster.Instance mahoutInstance = findMahoutInstance();
    Predicate<NodeMetadata> mahoutClientRole = and(alwaysTrue(), withIds(mahoutInstance.getId()));

    Map<? extends NodeMetadata, ExecResponse> responses = controller.runScriptOnNodesMatching(clusterSpec, mahoutClientRole, binMahout);

    LOG.info("Responses for Statement: " + binMahout);
    for (Map.Entry<? extends NodeMetadata, ExecResponse> entry : responses.entrySet()) {
      LOG.info("Node[" + entry.getKey().getId() + "]: " + entry.getValue());
    }

    assertResponsesContain(responses, binMahout, "Running on hadoop");
  }

  public static void assertResponsesContain(Map<? extends NodeMetadata, ExecResponse> responses, Statement statement, String text) {
    for (Map.Entry<? extends NodeMetadata, ExecResponse> entry : responses.entrySet()) {
        if (!entry.getValue().getOutput().contains(text)) {
            failNotEquals("Node: " + entry.getKey().getId()
                    + " failed to execute the command: " + statement
                    + " as could not find expected text", text, entry.getValue());
        }
    }
  }

  private Cluster.Instance findMahoutInstance() throws IOException {
      Cluster cluster = new ClusterStateStoreFactory().create(clusterSpec).load();
      return cluster.getInstanceMatching(anyRoleIn(newHashSet(MAHOUT_CLIENT_ROLE)));
  }
}
