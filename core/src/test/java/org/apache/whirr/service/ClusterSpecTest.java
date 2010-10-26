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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

public class ClusterSpecTest {
  
  @Test
  public void testDefaultsAreSet() throws ConfigurationException {
    ClusterSpec spec = new ClusterSpec();
    assertThat(spec.getRunUrlBase(),
        startsWith("http://whirr.s3.amazonaws.com/"));
  }

  @Test
  public void testDefaultsCanBeOverridden() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.RUN_URL_BASE.getConfigName(),
        "http://example.org");
    ClusterSpec spec = new ClusterSpec(conf);
    assertThat(spec.getRunUrlBase(), is("http://example.org"));
  }

  @Test
  public void testVersionInRunUrlbaseIsUrlEncoded()
      throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.VERSION.getConfigName(), "0.1.0+1");
    ClusterSpec spec = new ClusterSpec(conf);
    assertThat(spec.getRunUrlBase(),
        is("http://whirr.s3.amazonaws.com/0.1.0%2B1/"));
  }
}
