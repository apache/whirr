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

package org.apache.whirr.service;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

import java.net.MalformedURLException;

import org.junit.Test;

public class RunUrlBuilderTest {

  @Test
  public void testOnePath() throws MalformedURLException {
    assertThat(runUrls("http://example.org/", "a/b"),
        containsString("runurl http://example.org/a/b"));
  }
  
  @Test
  public void testOnePathNoSlash() throws MalformedURLException {
    assertThat(runUrls("http://example.org", "a/b"),
        containsString("runurl http://example.org/a/b"));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testTwoPaths() throws MalformedURLException {
    assertThat(runUrls("http://example.org/", "a/b", "c/d"), allOf(
        containsString("runurl http://example.org/a/b"),
        containsString("runurl http://example.org/c/d")));
  }

  @Test
  public void testAbsolutePath() throws MalformedURLException {
    assertThat(runUrls("http://example.org/", "http://example2.org/a/b"),
        containsString("runurl http://example2.org/a/b"));
  }

  private String runUrls(String runUrlBase, String... urls) throws MalformedURLException {
    return new String(RunUrlBuilder.runUrls(runUrlBase, urls));
  }
}
