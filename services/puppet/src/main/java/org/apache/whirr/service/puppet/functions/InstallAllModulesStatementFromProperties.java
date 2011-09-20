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

package org.apache.whirr.service.puppet.functions;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.contains;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;
import static org.apache.whirr.service.puppet.PuppetConstants.MODULE_KEY_PATTERN;
import static org.apache.whirr.service.puppet.predicates.PuppetPredicates.isModuleSubKey;

import java.util.Map;
import java.util.Set;

import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;


public class InstallAllModulesStatementFromProperties implements Function<Map<String, String>, Statement> {
   private final StatementToInstallModule statementMaker;

   public InstallAllModulesStatementFromProperties(StatementToInstallModule statementMaker) {
      this.statementMaker = checkNotNull(statementMaker, "statementMaker");
   }

   @Override
   public Statement apply(Map<String, String> moduleProps) {
      Set<String> allModules = ImmutableSet.copyOf(transform(
               filter(moduleProps.keySet(), contains(MODULE_KEY_PATTERN)), KeyToModuleNameOrNull.INSTANCE));

      Builder<Statement> statements = ImmutableSet.<Statement> builder();
      for (String module : allModules) {
         statements.add(statementMaker.apply(filterKeys(moduleProps, isModuleSubKey(module))));
      }
      StatementList installModules = new StatementList(statements.build());
      return installModules;
   }

}

