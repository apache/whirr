/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.whirr.service.puppet.PuppetConstants.MODULES_DIR;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

import java.net.URI;

import org.jclouds.javax.annotation.Nullable;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;

import com.google.common.collect.ImmutableSet;

/**
 * Class representing a single Puppet Module. Clones a Puppet module to /etc/puppet/modules.</p>
 * 
 * Typical usage is: <br/>
 * <blockquote>
 * 
 * <pre>
 * ...
 * 
 * @Override
 * protected void beforeBootstrap(ClusterActionEvent event) throws
 *    IOException, InterruptedException {
 * 
 *  ...
 * 
 *  nginx = new InstallModuleFromGit("nginx", "git://github.com/puppetlabs/puppetlabs-nginx.git")
 *  addStatement(event, nginx);
 * 
 *  OR
 * 
 *  fw = new InstallModuleFromGit("fw", "git://github.com/puppetlabs/puppetlabs-firewall.git", "next")
 *  addStatement(event, fw);
 * 
 *  ...
 * 
 * }
 * 
 * ...
 * </pre>
 * 
 * </blockquote>
 * 
 */
public class InstallModuleFromGit implements Statement {

  private final String module;
  private final URI url;
  private final String vcsBranch;

  /**
   * To clone the master branch of module from a repository located at <code>url</code>.
   * 
   * @param module
   * @param url
   */

  public InstallModuleFromGit(String module, URI url) {
    this(module, url, null);
  }

  public InstallModuleFromGit(String module, URI url, @Nullable String vcsBranch) {
    this.module = checkNotNull(module, "module");
    this.url = checkNotNull(url, "url");
    checkArgument(url.getScheme().equals("git"), "not a git url: %s", url);
    this.vcsBranch = vcsBranch;
  }

  @Override
  public Iterable<String> functionDependencies(OsFamily arg0) {
    return ImmutableSet.<String> of();
  }

  @Override
  public String render(OsFamily arg0) {
    return exec(
          "git clone " + (vcsBranch != null ? "-b " + vcsBranch + " " : "") + url + " " + MODULES_DIR
                + module).render(arg0);
  }
}

