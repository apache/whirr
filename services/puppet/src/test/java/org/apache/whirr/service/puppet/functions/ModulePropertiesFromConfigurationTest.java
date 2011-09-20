/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.puppet.functions;

import static junit.framework.Assert.assertEquals;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class ModulePropertiesFromConfigurationTest {

  @Test
  public void testWhenMatches() {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("puppet.nginx.module", "git://github.com/puppetlabs/puppetlabs-nginx.git");
    conf.setProperty("puppet.nginx.module.branch", "notmaster");
    conf.setProperty("puppet.foox.module", null);
    conf.setProperty("puppet.module", "foo");
    assertEquals(ImmutableMap.of("puppet.nginx.module.branch", "notmaster", "puppet.nginx.module",
          "git://github.com/puppetlabs/puppetlabs-nginx.git"), ModulePropertiesFromConfiguration.INSTANCE
          .apply(conf));
  }
}
