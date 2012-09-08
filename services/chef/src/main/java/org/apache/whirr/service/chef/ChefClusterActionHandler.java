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

package org.apache.whirr.service.chef;

import static org.apache.whirr.service.chef.ChefClusterActionHandlerFactory.CHEF_ROLE_PREFIX;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;

import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * Installs chef-solo provisioned services, chef itself and its dependencies.
 * 
 */
public class ChefClusterActionHandler extends ClusterActionHandlerSupport {

  public static final String CHEF_SLEEP_AFTER_RECIPE = "whirr.chef.recipe.sleep";
  public static final long CHEF_SLEEP_AFTER_RECIPE_DEFAULT = 2000L;

  private String role;
  private String cookbook;
  private String recipe;
  private long sleepAfterConfigure;

  public ChefClusterActionHandler(String role) {
    this.role = role;
    parseCookbookAndRecipe();
  }

  @Override
  public String getRole() {
    return role;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException,
      InterruptedException {
    addStatement(event, call("retry_helpers"));
    if (isFirstChefRoleIn(event.getInstanceTemplate().getRoles()).apply(role)) {
      addInstallChefStatements(event);
      // for some reason even non-running recipes like ant are sometimes not
      // immediately available after install. Jclouds seems to be behaving fine
      // and file system stores should be atomic wrt to visibility, but there
      // might be refresh or memory visibility issues wrt to how path context is
      // built or maintained. Until the issue is dug up a small sleep time
      // should take care of it
      sleepAfterConfigure = event.getClusterSpec().getConfiguration()
          .getLong(CHEF_SLEEP_AFTER_RECIPE, CHEF_SLEEP_AFTER_RECIPE_DEFAULT);
    }
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException,
      InterruptedException {
    handleFirewallRules(event);
    
    // if the role is an exact match to the prefix then there is nothing to
    // do (chef only installation)
    if (role.equals("")) {
      return;
    }
    addStatement(event, call("retry_helpers"));
    addStatement(event, new Recipe(cookbook, recipe, event.getClusterSpec()
        .getConfigurationForKeysWithPrefix(cookbook)));
  }

  private void addInstallChefStatements(ClusterActionEvent event) {
    // install ruby and ruby-gems in the nodes
    addStatement(event, call("install_ruby"));
    // install git
    addStatement(event, call("install_git"));
    // install chef-solo
    addStatement(event, call("install_chef"));
  }

  @Override
  protected void afterConfigure(ClusterActionEvent event) throws IOException,
      InterruptedException {
    Thread.sleep(sleepAfterConfigure);
  }

  private void parseCookbookAndRecipe() {
    String[] both = this.role.trim().split(":");
    if (both.length < 1 || both.length > 3) {
      throw new IllegalArgumentException(
          "Chef roles must be specified the following way: \"chef\" to install chef with no recipes,"
              + " \"chef:cookbook_name\" to install the default recipe in the provided cookbook,"
              + " or \"chef:cookbook_name:recipe_name\" to  install a particular recipe [was: "
              + role + "]");
    }
    switch (both.length) {
    case 3:
      recipe = both[2];
    case 2:
      cookbook = both[1];
    }
  }

  private static Predicate<String> isFirstChefRoleIn(
      final Iterable<String> roles) {
    return new Predicate<String>() {
      @Override
      public boolean apply(String arg0) {
        return Iterables.get(
            Iterables.filter(roles,
                Predicates.containsPattern("^" + CHEF_ROLE_PREFIX + arg0)), 0)
            .equals(CHEF_ROLE_PREFIX + arg0);
      }

      @Override
      public String toString() {
        return "isFirstChefRoleIn(" + roles + ")";

      }
    };
  }

}
