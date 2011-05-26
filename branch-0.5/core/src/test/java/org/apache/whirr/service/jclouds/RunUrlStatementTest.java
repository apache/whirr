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

package org.apache.whirr.service.jclouds;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.URL;

import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Test;

public class RunUrlStatementTest {
  
  private static final String NL = System.getProperty("line.separator");

  @Test
  public void testBaseWithTrailingSlash() throws IOException {
    assertThat(
        new RunUrlStatement(false, "http://example.org/", "a/b").render(OsFamily.UNIX),
        is("runurl http://example.org/a/b" + NL));
  }

  @Test
  public void testWithArgs() throws IOException {
    assertThat(
        new RunUrlStatement(false, "http://example.org/", "a/b", "x", "y").render(OsFamily.UNIX),
        is("runurl http://example.org/a/b x y" + NL));
  }
  
  @Test
  public void testBaseWithoutTrailingSlash() throws IOException {
    assertThat(
        new RunUrlStatement(false, "http://example.org", "a/b").render(OsFamily.UNIX),
        is("runurl http://example.org/a/b" + NL));
  }

  @Test
  public void testAbsolutePath() throws IOException {
    assertThat(
        new RunUrlStatement(false, "http://example.org/", "http://example2.org/a/b")
          .render(OsFamily.UNIX),
        is("runurl http://example2.org/a/b" + NL));
  }
  
  @Test
  public void testCheckUrlExists() throws IOException {
    RunUrlStatement.checkUrlExists(new URL("http://whirr.s3.amazonaws.com/"),
        "Exists");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckUrlDoesNotExist() throws IOException {
    RunUrlStatement.checkUrlExists(
        new URL("http://whirr.s3.amazonaws.com/non-existent"), "Doesn't exist");
  }

}
