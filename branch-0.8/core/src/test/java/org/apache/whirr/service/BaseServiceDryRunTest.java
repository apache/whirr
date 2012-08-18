/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service;

import com.google.common.base.Joiner;
import java.util.Set;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.InitScript;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.jcraft.jsch.JSchException;

public abstract class BaseServiceDryRunTest {

  protected ClusterSpec newClusterSpecForProperties(Map<String, String> properties) throws ConfigurationException,
      JSchException, IOException {
    Configuration config = new PropertiesConfiguration();
    config.setProperty("whirr.provider", "stub");
    config.setProperty("whirr.cluster-name", "stub-test");
    config.setProperty("whirr.state-store", "memory");
    for (Entry<String, String> entry : properties.entrySet())
      config.setProperty(entry.getKey(), entry.getValue());

    // we don't want to create files
    return new ClusterSpec(config) {
      @Override
      protected void checkAndSetKeyPair() {
        setPrivateKey("-----BEGIN RSA PRIVATE KEY-----");
        setPublicKey("ssh-rsa AAAAB3NzaC1yc2EA");
      }

    };
  }

  protected DryRun launchWithClusterSpec(ClusterSpec clusterSpec) throws IOException, InterruptedException {
    ClusterController controller = new ClusterController();
    DryRun dryRun = controller.getCompute().apply(clusterSpec).utils().injector().getInstance(DryRun.class);
    dryRun.reset();
    controller.launchCluster(clusterSpec);
    return dryRun;
  }

  /**
   * Tests that a simple cluster is correctly loaded and executed.
   */
  @Test
  public void testBootstrapAndConfigure() throws Exception {
    ClusterSpec cookbookWithDefaultRecipe = newClusterSpecForProperties(ImmutableMap.of(
        "whirr.instance-templates", "1 " + Joiner.on("+").join(getInstanceRoles())));
    DryRun dryRun = launchWithClusterSpec(cookbookWithDefaultRecipe);

    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());
    assertScriptPredicateOnPhase(dryRun, "configure", configurePredicate());
  }

  protected abstract Predicate<CharSequence> configurePredicate();

  protected abstract Predicate<CharSequence> bootstrapPredicate();

  protected abstract Set<String> getInstanceRoles();

  protected void assertScriptPredicateOnPhase(DryRun dryRun, String phase, Predicate<CharSequence> predicate)
      throws Exception {
    assertScriptPredicate(getEntryForPhase(dryRun.getExecutions(), phase), predicate);
  }

  protected void assertScriptPredicate(Entry<NodeMetadata, Statement> setup, Predicate<CharSequence> predicate) {
    assertTrue("The predicate did not match", predicate.apply(setup.getValue().render(OsFamily.UNIX)));
  }

  protected void assertNoEntryForPhase(DryRun dryRun, String phaseName) throws Exception {
    try {
      fail("Found entry: " + getEntryForPhase(dryRun.getExecutions(), phaseName));

    } catch (IllegalStateException e) {
      // No entry found - OK
    }
  }

  protected Entry<NodeMetadata, Statement> getEntryForPhase(ListMultimap<NodeMetadata, Statement> listMultimap,
      String phaseName) throws Exception {
    for (Entry<NodeMetadata, Statement> entry : listMultimap.entries()) {
      if (getScriptName(entry.getValue()).startsWith(phaseName)) {
        return entry;
      }
    }
    throw new IllegalStateException("phase not found: " + phaseName);
  }

  protected String getScriptName(Statement statement) throws Exception {
    return InitScript.class.cast(statement).getInstanceName();
  }
}
