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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

public class VersionCommandTest {

  private ByteArrayOutputStream outBytes;
  private PrintStream out;

  @Before
  public void setUp() {
    outBytes = new ByteArrayOutputStream();
    out = new PrintStream(outBytes);
  }

  @Test
  public void testOverrides() throws Exception {
    VersionCommand command = new VersionCommand();
    int rc = command.run(null, out, null, Collections.<String>emptyList());
    assertThat(rc, is(0));
    assertThat(outBytes.toString(), startsWith("Apache Whirr "));
  }
  
}
