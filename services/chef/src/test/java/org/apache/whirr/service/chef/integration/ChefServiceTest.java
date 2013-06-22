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

package org.apache.whirr.service.chef.integration;

import static org.jclouds.util.Predicates2.retry;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.chef.Recipe;
import org.jclouds.compute.RunScriptOnNodesException;
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
import java.util.concurrent.TimeUnit;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.failNotEquals;

/**
 * Integration test for chef.
 */
public class ChefServiceTest {

  private static Predicate<NodeMetadata> allNodes = Predicates.alwaysTrue();

  private static final Logger LOG = LoggerFactory
      .getLogger(ChefServiceTest.class);

  private static ClusterSpec clusterSpec;
  private static ClusterController controller;

  // private static Cluster cluster;

  @BeforeClass
  public static void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System
          .getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration(
        "whirr-chef-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    controller.launchCluster(clusterSpec);
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testRecipesWereRanInServiceBootstrap() throws Exception {

    // and shoudl be installed by the main handlers
    Statement testAnt = Statements.exec("ant -version");

    Map<? extends NodeMetadata, ExecResponse> responses = controller
        .runScriptOnNodesMatching(clusterSpec, allNodes, testAnt);

    printResponses(testAnt, responses);

    assertResponsesContain(responses, testAnt, "Apache Ant");

    Statement testMaven = Statements.exec("mvn --version");

    responses = controller.runScriptOnNodesMatching(clusterSpec, allNodes,
        testMaven);

    printResponses(testMaven, responses);

    assertResponsesContain(responses, testMaven, "Apache Maven");
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testChefRunRecipeFromURL() throws Exception {
    // As chef will be mostly used indirectly in other services
    // this test tests chef's ability to run a recipe, specifically to
    // install nginx.
    Recipe nginx = new Recipe(
        "nginx",
        null,
        "http://s3.amazonaws.com/opscode-community/cookbook_versions/tarballs/529/original/nginx.tgz");

    Map<? extends NodeMetadata, ExecResponse> responses = runRecipe(nginx);

    printResponses(nginx, responses);

    HttpClient client = new HttpClient();
    final GetMethod getIndex = new GetMethod(String.format("http://%s",
        responses.keySet().iterator().next().getPublicAddresses().iterator()
            .next()));

    assertTrue("Could not connect with nginx server",
               retry(new Predicate<HttpClient>() {

          @Override
          public boolean apply(HttpClient input) {
            try {
              int statusCode = input.executeMethod(getIndex);
              assertEquals("Status code should be 200", HttpStatus.SC_OK,
                  statusCode);
              return true;
            } catch (Exception e) {
              return false;
            }
          }

        }, 10, 1, TimeUnit.SECONDS).apply(client));

    String indexPageHTML = getIndex.getResponseBodyAsString();
    assertTrue("The string 'nginx' should appear on the index page",
        indexPageHTML.contains("nginx"));
  }

  /**
   * Test the execution of recipes (one with attribs and one with non-default
   * recipe) from the provided recipes dowloaded from opscode's git repo.
   * 
   * @throws Exception
   */
  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testChefRunRecipesFromProvidedCookbooks() throws Exception {
    Recipe java = new Recipe("java");
    java.attribs.put("install_flavor", "openjdk");

    // Recipes have to run directly against ComputeService as they need to be
    // ran as initscripts, a future version of ClusterController might avoid
    // taht.

    Map<? extends NodeMetadata, ExecResponse> responses = runRecipe(java);

    printResponses(java, responses);

    Statement stmt = Statements.exec("java -version");

    responses = controller
        .runScriptOnNodesMatching(clusterSpec, allNodes, stmt);

    assertResponsesContain(responses, stmt, "Runtime Environment");

    Recipe postgreSql = new Recipe("postgresql", "server");

    responses = runRecipe(postgreSql);

    printResponses(postgreSql, responses);

    stmt = Statements.exec("psql --version");

    responses = controller
        .runScriptOnNodesMatching(clusterSpec, allNodes, stmt);

    assertResponsesContain(responses, stmt, "PostgreSQL");
  }
  
  

  private Map<? extends NodeMetadata, ExecResponse> runRecipe(Recipe recipe)
      throws IOException, RunScriptOnNodesException {
    return controller.runScriptOnNodesMatching(clusterSpec, allNodes, recipe,
        Recipe.recipeScriptOptions(controller
            .defaultRunScriptOptionsForSpec(clusterSpec)));
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (controller != null) {
      controller.destroyCluster(clusterSpec);
    }
  }

  public static void assertResponsesContain(
      Map<? extends NodeMetadata, ExecResponse> responses, Statement statement,
      String text) {
    for (Map.Entry<? extends NodeMetadata, ExecResponse> entry : responses
        .entrySet()) {
      if (!entry.getValue().getOutput().contains(text)) {
        failNotEquals("Node: " + entry.getKey().getId()
            + " failed to execute the command: " + statement
            + " as could not find expected text", text, entry.getValue());
      }
    }
  }

  public static void printResponses(Statement statement,
      Map<? extends NodeMetadata, ExecResponse> responses) {
    LOG.info("Responses for Statement: " + statement);
    for (Map.Entry<? extends NodeMetadata, ExecResponse> entry : responses
        .entrySet()) {
      LOG.info("Node[" + entry.getKey().getId() + "]: " + entry.getValue());
    }
  }
}
