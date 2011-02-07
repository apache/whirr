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

package org.apache.whirr.service.jclouds;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.List;

import org.jclouds.scriptbuilder.ScriptBuilder;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;

public class StatementBuilder implements Statement {
  protected List<Statement> statements = Lists.newArrayList();
  
  public void addStatement(Statement statement) {
    if (!statements.contains(statement)) {
      statements.add(statement);
    }
  }
  
  @Override
  public Iterable<String> functionDependecies(OsFamily family) {
     List<String> functions = Lists.newArrayList();
     for (Statement statement : statements) {
        Iterables.addAll(functions, statement.functionDependecies(family));
     }
     return functions;
  }

  @Override
  public String render(OsFamily family) {
    ScriptBuilder scriptBuilder = new ScriptBuilder();
    for (Statement statement : statements) {
      scriptBuilder.addStatement(statement);
    }
    return scriptBuilder.render(family);
  }

}
