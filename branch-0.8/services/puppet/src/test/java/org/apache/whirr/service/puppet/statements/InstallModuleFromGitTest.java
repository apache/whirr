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

import java.net.URI;

import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Test;

public class InstallModuleFromGitTest {

  @Test
  public void testWhenModuleAndURIValid() {

    InstallModuleFromGit nginx = new InstallModuleFromGit("nginx", URI
          .create("git://github.com/puppetlabs/puppetlabs-nginx.git"));

    assertEquals("git clone git://github.com/puppetlabs/puppetlabs-nginx.git /etc/puppet/modules/nginx\n", nginx
          .render(OsFamily.UNIX));
  }

  @Test
  public void testWhenModuleAndURIAndBranchValid() {

    InstallModuleFromGit nginx = new InstallModuleFromGit("nginx", URI
          .create("git://github.com/puppetlabs/puppetlabs-nginx.git"), "WHIRR-385");

    assertEquals("git clone -b WHIRR-385 git://github.com/puppetlabs/puppetlabs-nginx.git /etc/puppet/modules/nginx\n", nginx
          .render(OsFamily.UNIX));
  }
}

