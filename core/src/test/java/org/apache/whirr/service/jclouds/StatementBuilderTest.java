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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.ClusterSpec;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statements;
import org.junit.Test;

public class StatementBuilderTest {
  
  @Test
  public void testDeduplication() throws Exception {
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys();

    clusterSpec.setClusterName("test-cluster");
    clusterSpec.setProvider("test-provider");

    StatementBuilder builder = new StatementBuilder();

    builder.addStatement(
        new RunUrlStatement(false, "http://example.org/", "a/b", "c"));
    builder.addStatement(
        new RunUrlStatement(false, "http://example.org/", "d/e", "f"));
    builder.addStatement(
        new RunUrlStatement(false, "http://example.org/", "a/b", "c"));

    String script = builder.name("foo").build(clusterSpec).render(OsFamily.UNIX);
    int first = script.indexOf("runurl http://example.org/a/b c");
    assertThat(first, greaterThan(-1));

    int second = script.indexOf("runurl http://example.org/a/b c", first + 1);
    assertThat("No second occurrence", second, is(-1));
    assertThat(script, containsString("runurl http://example.org/d/e f"));
  }

  @Test
  public void testExplicitExports() throws Exception {
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys();

    clusterSpec.setClusterName("test-cluster");
    clusterSpec.setProvider("test-provider");

    StatementBuilder builder = new StatementBuilder();

    builder.addExport("fooBar", "my_value");
    builder.addStatement(Statements.exec("echo $FOO_BAR"));

    String script = builder.name("foo").build(clusterSpec).render(OsFamily.UNIX);
    
    assertThat(script, containsString("export FOO_BAR="));
    assertThat(script, containsString("my_value"));
    assertThat(script, containsString("echo $FOO_BAR"));
  }

  @Test
  public void testClusterSpecExports() throws Exception {
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.addProperty("ignored", "ignored_value");
    conf.addProperty("whirr.env.fooBar", "my_value");
    
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(conf);
    clusterSpec.setClusterName("test-cluster");
    clusterSpec.setProvider("test-provider");

    StatementBuilder builder = new StatementBuilder();

    builder.addStatement(Statements.exec("echo $FOO_BAR"));
    
    String script = builder.name("foo").build(clusterSpec).render(OsFamily.UNIX);
    
    assertThat(script, containsString("export FOO_BAR="));
    assertThat(script, containsString("my_value"));
    assertThat(script, containsString("echo $FOO_BAR"));
    assertThat(script, not(containsString("ignored")));
  }

}
