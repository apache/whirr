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

import java.io.IOException;

import org.apache.whirr.service.puppet.functions.StatementToInstallModule.PrepareRemoteFileUrl;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class StatementToInstallModuleTest {
  StatementToInstallModule getStatement = new StatementToInstallModule(new PrepareRemoteFileUrl() {

    @Override
    public String apply(String rawUrl) throws IOException {
      return rawUrl;
    }

  });

  @Test
  public void testWhenRemote() {

    assertEquals("install_tarball remote://path/to/tgz /etc/puppet/modules/nginx || return 1\n", getStatement.apply(
         ImmutableMap.of("puppet.nginx.module", "remote://path/to/tgz"))
          .render(OsFamily.UNIX));
  }

  @Test
  public void testWhenFile() {

    assertEquals("install_tarball file://path/to/tgz /etc/puppet/modules/nginx || return 1\n", getStatement.apply(
         ImmutableMap.of("puppet.nginx.module", "file://path/to/tgz"))
          .render(OsFamily.UNIX));
  }

  @Test
  public void testWhenGit() {

    assertEquals("git clone git://github.com/puppetlabs/puppetlabs-nginx.git /etc/puppet/modules/nginx\n",
          getStatement.apply(ImmutableMap.of("puppet.nginx.module",
                      "git://github.com/puppetlabs/puppetlabs-nginx.git")).render(OsFamily.UNIX));
  }
  
  @Test
  public void testWhenGitAndBranch() {

    assertEquals("git clone -b notmaster git://github.com/puppetlabs/puppetlabs-nginx.git /etc/puppet/modules/nginx\n",
          getStatement.apply(ImmutableMap.of("puppet.nginx.module.branch", "notmaster", "puppet.nginx.module",
                      "git://github.com/puppetlabs/puppetlabs-nginx.git")).render(OsFamily.UNIX));
  }
}

