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

import org.apache.whirr.service.BaseServiceDryRunTest;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.junit.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class KerberosClientDryRunTest extends BaseServiceDryRunTest {

  @Override
  protected Set<String> getInstanceRoles() {
    return ImmutableSet.of(KerberosServerHandler.ROLE + "+" + KerberosClientHandler.ROLE);
  }

  @Override
  protected Predicate<CharSequence> bootstrapPredicate() {
    return and(containsPattern("configure_hostnames"),
      and(containsPattern("java"), containsPattern("install_kerberos_client")));
  }

  @Override
  protected Predicate<CharSequence> configurePredicate() {
    return containsPattern("configure_kerberos_client");
  }

  @Test
  public void testNoServer() throws Exception {
    DryRun dryRun = launchWithClusterSpec(newClusterSpecForProperties(ImmutableMap.of("whirr.instance-templates", "1 "
      + KerberosClientHandler.ROLE)));
    assertScriptPredicateOnPhase(dryRun, "bootstrap", bootstrapPredicate());
  }
  
}
