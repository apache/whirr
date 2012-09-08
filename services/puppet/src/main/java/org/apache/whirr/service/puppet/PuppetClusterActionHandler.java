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

package org.apache.whirr.service.puppet;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.whirr.service.puppet.PuppetConstants.PUPPET;
import static org.apache.whirr.service.puppet.predicates.PuppetPredicates.isFirstPuppetRoleIn;
import static org.apache.whirr.service.puppet.predicates.PuppetPredicates.isLastPuppetRoleIn;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.puppet.functions.InstallAllModulesStatementFromProperties;
import org.apache.whirr.service.puppet.functions.ModulePropertiesFromConfiguration;
import org.apache.whirr.service.puppet.functions.RolesManagedByPuppet;
import org.apache.whirr.service.puppet.functions.StatementToInstallModule;
import org.apache.whirr.service.puppet.statements.CreateSitePpAndApplyRoles;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

/**
 * Installs puppet. After this service is configured other services can use puppet to setup/start
 * other services.
 * 
 */
public class PuppetClusterActionHandler extends PuppetInstallClusterActionHandler {
  static final Logger LOG = LoggerFactory.getLogger(PuppetClusterActionHandler.class);

  private final String role;
  private final Function<ClusterActionEvent, StatementToInstallModule> getStatementToInstallModuleForAction;

  public PuppetClusterActionHandler(String role) {
    this(role, new Function<ClusterActionEvent, StatementToInstallModule>() {

      @Override
      public StatementToInstallModule apply(ClusterActionEvent arg0) {
        return new StatementToInstallModule(arg0);
      }

    });
  }

  /**
   * @param getStatementToInstallModuleForAction allows you to override to facilitate testing
   */
  public PuppetClusterActionHandler(String role,
        Function<ClusterActionEvent, StatementToInstallModule> getStatementToInstallModuleForAction) {
    this.role = checkNotNull(role, "role");
    this.getStatementToInstallModuleForAction = checkNotNull(getStatementToInstallModuleForAction,
          "getStatementToInstallModuleForAction");
  }

  @Override
  public String getRole() {
    return role;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    if (isFirstPuppetRoleIn(event.getInstanceTemplate().getRoles()).apply(getRole())) {
      // install puppet when bootstrapping the first puppet role
      super.beforeBootstrap(event);
      installAllKnownModules(event);
    }

  }

  private void installAllKnownModules(ClusterActionEvent event) throws IOException {
    Map<String, String> moduleProps = ModulePropertiesFromConfiguration.INSTANCE.apply(event.getClusterSpec()
          .getConfigurationForKeysWithPrefix(PUPPET));

    StatementToInstallModule statementMaker = getStatementToInstallModuleForAction.apply(event);
    Statement installModules = new InstallAllModulesStatementFromProperties(statementMaker).apply(moduleProps);

    event.getStatementBuilder().addStatement(installModules);
    LOG.debug("Puppet finished installing modules for " + event.getInstanceTemplate());
  }

  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
    handleFirewallRules(event);
    
    super.beforeConfigure(event);
    
    if (isLastPuppetRoleIn(event.getInstanceTemplate().getRoles()).apply(getRole())) {
      Configuration config = event.getClusterSpec().getConfiguration();
      Iterable<String> roles = RolesManagedByPuppet.INSTANCE.apply(event.getInstanceTemplate().getRoles());
      addStatement(event, new CreateSitePpAndApplyRoles(roles, config));
    }

  }
}
