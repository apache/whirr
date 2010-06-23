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

  private static final String WHIRR_RUNURL_BASE = "whirr.runurl.base";

  @Test
  public void testOnePath() throws MalformedURLException {
    assertThat(runUrls("/a/b"),
        containsString("runurl http://whirr.s3.amazonaws.com/a/b"));
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void testTwoPaths() throws MalformedURLException {
    assertThat(runUrls("/a/b", "/c/d"), allOf(
        containsString("runurl http://whirr.s3.amazonaws.com/a/b"),
        containsString("runurl http://whirr.s3.amazonaws.com/c/d")));
  }

  @Test
  public void testAbsolutePath() throws MalformedURLException {
    assertThat(runUrls("http://example.org/a/b"),
        containsString("runurl http://example.org/a/b"));
  }
  
  @Test
  public void testSystemOverrideOfRunUrlBaseNoSlash() throws MalformedURLException {
    String prev = System.setProperty(WHIRR_RUNURL_BASE, "http://example.org");
    assertThat(runUrls("/a/b"),
        containsString("runurl http://example.org/a/b"));
    if (prev == null) {
      System.clearProperty(WHIRR_RUNURL_BASE);
    } else {
      System.setProperty(WHIRR_RUNURL_BASE, prev);
    }
  }
  
  @Test
  public void testSystemOverrideOfRunUrlBaseWithSlash() throws MalformedURLException {
    String prev = System.setProperty(WHIRR_RUNURL_BASE, "http://example.org/");
    assertThat(runUrls("/a/b"),
        containsString("runurl http://example.org/a/b"));
    if (prev == null) {
      System.clearProperty(WHIRR_RUNURL_BASE);
    } else {
      System.setProperty(WHIRR_RUNURL_BASE, prev);
    }
  }
  
  private String runUrls(String... urls) throws MalformedURLException {
    return new String(RunUrlBuilder.runUrls(urls));
  }
}
