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

package org.apache.whirr.service.kerberos;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.containsPattern;

import java.util.Set;

import junit.framework.AssertionFailedError;

import org.apache.whirr.service.BaseServiceDryRunTest;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class KerberosServerDryRunTest extends BaseServiceDryRunTest {

  @Override
  protected Set<String> getInstanceRoles() {
    return ImmutableSet.of(KerberosServerHandler.ROLE);
  }

  @Override
  protected Predicate<CharSequence> bootstrapPredicate() {
    return and(containsPattern("configure_hostnames"),
      and(containsPattern("java"), containsPattern("install_kerberos_server")));
  }

  @Override
  protected Predicate<CharSequence> configurePredicate() {
    return containsPattern("configure_kerberos_server");
  }

  @Test
  public void testKerberosRealm() throws Exception {
    DryRun dryRun = launchWithClusterSpec(newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates", "1 "
      + KerberosServerHandler.ROLE)));
    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());
    assertScriptPredicateOnPhase(dryRun, "configure", configurePredicate());
  }

  @Test
  public void testJavaInstalled() throws Exception {
    DryRun dryRun = launchWithClusterSpec(newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates", "1 "
        + KerberosServerHandler.ROLE + "+" + KerberosClientHandler.ROLE)));
    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());
    assertScriptPredicateOnPhase(dryRun, "bootstrap", containsPattern("install_openjdk"));
  }

  @Test
  public void testJavaInstalledFalse() throws Exception {
    DryRun dryRun = launchWithClusterSpec(newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates", "1 "
        + KerberosServerHandler.ROLE + "+" + KerberosClientHandler.ROLE, "whirr.env.jdk_installed", "false")));
    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());
    assertScriptPredicateOnPhase(dryRun, "bootstrap", containsPattern("install_openjdk"));
  }

  @Test
  public void testJavaInstalledTrue() throws Exception {
    DryRun dryRun = launchWithClusterSpec(newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates", "1 "
        + KerberosServerHandler.ROLE + "+" + KerberosClientHandler.ROLE, "whirr.env.jdk_installed", "true")));
    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());
    boolean assertFailed = false;
    try {
      assertScriptPredicateOnPhase(dryRun, "bootstrap", containsPattern("install_openjdk"));
    } catch (AssertionFailedError assertionFailedError) {
      assertFailed = true;
    }
    Assert.assertTrue(assertFailed);
  }

}
