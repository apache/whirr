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

package org.apache.whirr.service.hadoop;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;

import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

public class HadoopConfigurationConverterTest {

  @Test
  public void testConversion() {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("p1", "v1,v2");
    conf.setProperty("p2", "v3");
    List<String> lines = HadoopConfigurationConverter.asXmlConfigurationLines(conf);
    assertThat(lines, is((List<String>) Lists.newArrayList(
        "<configuration>",
        "  <property>",
        "    <name>p1</name>",
        "    <value>v1,v2</value>",
        "  </property>",
        "  <property>",
        "    <name>p2</name>",
        "    <value>v3</value>",
        "  </property>",
        "</configuration>"
    )));
  }

  @Test
  public void testFinal() {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("p1", "v1");
    conf.setProperty("p2", "v2");
    conf.setProperty("p2.final", "true");
    List<String> lines = HadoopConfigurationConverter.asXmlConfigurationLines(conf);
    assertThat(lines, is((List<String>) Lists.newArrayList(
        "<configuration>",
        "  <property>",
        "    <name>p1</name>",
        "    <value>v1</value>",
        "  </property>",
        "  <property>",
        "    <name>p2</name>",
        "    <value>v2</value>",
        "    <final>true</final>",
        "  </property>",
        "</configuration>"
    )));
  }

}
