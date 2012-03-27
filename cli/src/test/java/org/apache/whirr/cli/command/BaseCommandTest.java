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

package org.apache.whirr.cli.command;

import static junit.framework.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;

import org.apache.whirr.service.DryRunModule;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.InitScript;
import org.jclouds.scriptbuilder.domain.Statement;
import org.junit.Before;

import com.google.common.collect.ListMultimap;

public class BaseCommandTest {

  protected ByteArrayOutputStream outBytes;
  protected PrintStream out;

  protected ByteArrayOutputStream errBytes;
  protected PrintStream err;

  @Before
  public void setUp() {
    outBytes = new ByteArrayOutputStream();
    out = new PrintStream(outBytes);

    errBytes = new ByteArrayOutputStream();
    err = new PrintStream(errBytes);
  }

  protected void assertNoEntryForPhases(DryRunModule.DryRun dryRun, String... phases) throws Exception {
    for (String phaseName : phases) {
      try {
        fail("Found entry: " + getEntryForPhase(dryRun.getExecutions(), phaseName) + " for phase: " + phaseName);
      } catch (IllegalStateException e) {
        // No entry found - OK
      }
    }
  }

  protected void assertExecutedPhases(DryRunModule.DryRun dryRun, String... phases) throws Exception {
    for (String phaseName : phases) {
      try {
        getEntryForPhase(dryRun.getExecutions(), phaseName);
      } catch (IllegalStateException e) {
        fail("No entry found for phase: " + phaseName);
      }
    }
  }

  private Map.Entry<NodeMetadata, Statement> getEntryForPhase(
      ListMultimap<NodeMetadata, Statement> executions, String phaseName)
      throws Exception {
    for (Map.Entry<NodeMetadata, Statement> entry : executions.entries()) {
      if (getScriptName(entry.getValue()).startsWith(phaseName)) {
        return entry;
      }
    }
    throw new IllegalStateException("phase not found: " + phaseName);
  }

  private String getScriptName(Statement script) throws Exception {
    return InitScript.class.cast(script).getInstanceName();
  }
}
