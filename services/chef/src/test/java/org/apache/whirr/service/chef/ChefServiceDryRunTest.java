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

package org.apache.whirr.service.chef;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.DryRunModule;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.callables.RunScriptOnNodeAsInitScriptUsingSsh;
import org.jclouds.compute.callables.RunScriptOnNodeAsInitScriptUsingSshAndBlockUntilComplete;
import org.jclouds.compute.callables.SudoAwareInitManager;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.InitBuilder;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ListMultimap;
import com.jcraft.jsch.JSchException;

public class ChefServiceDryRunTest {

  private Configuration chefOnly;
  private Configuration cookbookWithDefaultRecipe;
  private Configuration cookbookWithSpecificRecipe;
  private Configuration cookbookWithAttributes;

  @Before
  public void setUp() {

    DryRunModule.resetDryRun();

    chefOnly = newConfig();
    chefOnly.setProperty("whirr.instance-templates", "1 chef");

    cookbookWithDefaultRecipe = newConfig();
    cookbookWithDefaultRecipe.setProperty("whirr.instance-templates",
        "1 chef:java");

    cookbookWithSpecificRecipe = newConfig();
    cookbookWithSpecificRecipe.setProperty("whirr.instance-templates",
        "1 chef:java:sun");

    cookbookWithAttributes = newConfig();
    cookbookWithAttributes.setProperty("whirr.instance-templates",
        "1 chef:java");
    cookbookWithAttributes.addProperty("java.url", "http://testurl");
    cookbookWithAttributes.addProperty("java.version", "1.5");
    cookbookWithAttributes.addProperty("java.flavor", "vanilla");
  }

  private Configuration newConfig() {
    Configuration config = new PropertiesConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.state-store", "memory");
    return config;
  }

  private ClusterController launchWithConfig(Configuration config)
      throws IOException, InterruptedException, ConfigurationException,
      JSchException {
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);
    ClusterController controller = new ClusterController();
    controller.launchCluster(clusterSpec);
    return controller;
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefOnly() throws Exception {
    launchWithConfig(chefOnly);
    assertInstallFunctionsWereExecuted(DryRun.INSTANCE);
    assertNoEntryForPhase(DryRun.INSTANCE, "configure");
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefWithDefaultRecipe() throws Exception {
    launchWithConfig(cookbookWithDefaultRecipe);
    assertInstallFunctionsWereExecuted(DryRun.INSTANCE);
    // chef execution with a default cookbook recipe should contain a
    // particular string
    assertScriptPredicateOnPhase(DryRun.INSTANCE, "configure",
        new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            return input.contains("chef-solo -j /tmp/java::default");
          }
        });
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefWithAttributes() throws Exception {
    launchWithConfig(cookbookWithSpecificRecipe);
    assertInstallFunctionsWereExecuted(DryRun.INSTANCE);
    // chef execution with a default cookbook recipe should contain a
    // particular string
    assertScriptPredicateOnPhase(DryRun.INSTANCE, "configure",
        new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            return input.contains("chef-solo -j /tmp/java::sun");
          }
        });
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefWithParticularRecipe() throws Exception {
    launchWithConfig(cookbookWithAttributes);
    assertInstallFunctionsWereExecuted(DryRun.INSTANCE);
    // chef execution with a default cookbook recipe should contain a
    // particular string
    assertScriptPredicateOnPhase(DryRun.INSTANCE, "configure",
        new Predicate<String>() {
          @Override
          public boolean apply(String input) {
            return input
                .contains("{\"java\":{\"version\":\"1.5\",\"flavor\":\"vanilla\"}");
          }
        });
  }

  private void assertScriptPredicateOnPhase(DryRun dryRun, String phase,
      Predicate<String> predicate) throws Exception {
    assertScriptPredicate(getEntryForPhase(dryRun.getExecutions(), phase),
        predicate);
  }

  private void assertScriptPredicate(
      Entry<NodeMetadata, RunScriptOnNode> entry, Predicate<String> predicate) {
    assertTrue("The predicate did not match",
        predicate.apply(entry.getValue().getStatement().render(OsFamily.UNIX)));
  }

  private void assertInstallFunctionsWereExecuted(DryRun dryRun)
      throws Exception {

    Entry<NodeMetadata, RunScriptOnNode> setup = getEntryForPhase(
        dryRun.getExecutions(), "setup-");

    assertScriptPredicate(setup, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return input.contains("install_ruby");
      }
    });

    assertScriptPredicate(setup, new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return input.contains("install_chef");
      }
    });
  }

  private void assertNoEntryForPhase(DryRun dryRun, String phaseName) throws Exception {
    try {
      fail("Found entry: " + getEntryForPhase(dryRun.getExecutions(), phaseName));

    } catch (IllegalStateException e) {
      // No entry found - OK
    }
  }

  private Entry<NodeMetadata, RunScriptOnNode> getEntryForPhase(
      ListMultimap<NodeMetadata, RunScriptOnNode> executions, String phaseName)
      throws Exception {
    for (Entry<NodeMetadata, RunScriptOnNode> entry : executions.entries()) {
      if (getScriptName(entry.getValue()).startsWith(phaseName)) {
        return entry;
      }
    }
    throw new IllegalStateException("phase not found: " + phaseName);
  }

  private String getScriptName(RunScriptOnNode script) throws Exception {
    if (script instanceof RunScriptOnNodeAsInitScriptUsingSsh) {
      Field initField = SudoAwareInitManager.class
          .getDeclaredField("init");
      initField.setAccessible(true);
      return ((InitBuilder) initField
          .get((RunScriptOnNodeAsInitScriptUsingSshAndBlockUntilComplete) script))
          .getInstanceName();
    }
    throw new IllegalArgumentException();
  }
}
