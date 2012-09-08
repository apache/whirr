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

package org.apache.whirr.service.chef;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.contains;
import static com.google.common.base.Predicates.containsPattern;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import static java.util.regex.Pattern.LITERAL;
import static java.util.regex.Pattern.compile;

import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.BaseServiceDryRunTest;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

public class ChefServiceDryRunTest extends BaseServiceDryRunTest {

  @Override
  protected Set<String> getInstanceRoles() {
    return ImmutableSet.of("chef:java");
  }

  @Override
  protected Predicate<CharSequence> bootstrapPredicate() {
    return and(containsPattern("install_ruby"), containsPattern("install_chef"));
  }

  @Override
  protected Predicate<CharSequence> configurePredicate() {
    return contains(compile("{\"run_list\":[\"recipe[java]\"]}", LITERAL));
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefOnly() throws Exception {
    ClusterSpec chefOnly = newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates", "1 chef"));
    DryRun dryRun = launchWithClusterSpec(chefOnly);
    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());
    // We now have iptables calls by default in the configure phase.
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefWithDefaultRecipe() throws Exception {
    ClusterSpec cookbookWithDefaultRecipe = newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates",
        "1 chef:java"));
    DryRun dryRun = launchWithClusterSpec(cookbookWithDefaultRecipe);

    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());

    // chef execution with a default cookbook recipe should contain a
    // particular string
    assertScriptPredicateOnPhase(dryRun, "configure",
      containsPattern("chef-solo -j /tmp/java::default"));
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefWithAttributes() throws Exception {
    ClusterSpec cookbookWithSpecificRecipe = newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates",
        "1 chef:java:sun"));
    DryRun dryRun = launchWithClusterSpec(cookbookWithSpecificRecipe);

    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());

    // chef execution with a default cookbook recipe should contain a
    // particular string
    assertScriptPredicateOnPhase(dryRun, "configure",
      containsPattern("chef-solo -j /tmp/java::sun"));
  }

  /**
   * Tests that a simple recipe (the default recipe in a cookbook without any
   * configuration parameters) is correctly loaded and executed.
   */
  @Test
  public void testChefWithParticularRecipe() throws Exception {
    ClusterSpec cookbookWithAttributes = newClusterSpecForProperties(ImmutableMap.<String, String> builder()
        .put("whirr.instance-templates", "1 chef:java").put("java.url", "http://testurl")
        .put("java.version", "1.5").put("java.flavor", "vanilla").build());
    DryRun dryRun = launchWithClusterSpec(cookbookWithAttributes);

    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());

    // chef execution with a default cookbook recipe should contain a
    // particular string
    assertScriptPredicateOnPhase(dryRun, "configure",
      contains(compile("{\"java\":{\"version\":\"1.5\",\"flavor\":\"vanilla\"}", LITERAL)));
  }

}
