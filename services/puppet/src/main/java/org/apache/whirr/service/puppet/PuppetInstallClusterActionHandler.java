/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.puppet;

import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;

import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

/**
 * Installs puppet (and ruby). 
 * After this service is configured other services can use it to setup/start other services.
 * 
 * To test manually, run whirr launch-cluster with 1 puppet node, then ssh and confirm puppet exists
 */
public class PuppetInstallClusterActionHandler extends ClusterActionHandlerSupport {

  public static final String PUPPET_INSTALL_ROLE = "puppet-install";

  @Override
  public String getRole() {
   return PUPPET_INSTALL_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException,
    InterruptedException {
   
   // install ruby and ruby-gems in the nodes
   addStatement(event, call("install_ruby"));

   // install git
   addStatement(event, call("install_git"));

   // install puppet
   addStatement(event, call("install_puppet"));
  }
}
