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

package org.apache.whirr.service.puppet.statements;

import static junit.framework.Assert.assertEquals;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.CharStreams;
import com.google.common.io.Resources;

public class CreateSitePpAndApplyRolesTest {

  @Test
  public void testWithAttribs() throws IOException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("puppet.nginx.module", "git://github.com/puppetlabs/puppetlabs-nginx.git");
    conf.setProperty("nginx.server.hostname", "foohost");

    CreateSitePpAndApplyRoles nginx = new CreateSitePpAndApplyRoles(ImmutableSet.of("nginx::server"), conf);

    assertEquals(CharStreams.toString(Resources.newReaderSupplier(Resources.getResource("nginx-with-attribs.txt"),
          Charsets.UTF_8)), nginx.render(OsFamily.UNIX));
  }

}
