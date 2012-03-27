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

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.scriptbuilder.InitScript;
import org.jclouds.scriptbuilder.domain.Statement;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class StatementBuilder {
  protected String name;
  protected List<Statement> initStatements = Lists.newArrayList();
  protected List<Statement> statements = Lists.newArrayList();
  protected Map<String, String> exports = Maps.newLinkedHashMap();
  protected Map<String, Map<String, String>> exportsByInstanceId = Maps.newHashMap();

  public StatementBuilder addStatement(Statement statement) {
    if (!statements.contains(statement)) {
      statements.add(statement);
    }
    return this;
  }
  
  public StatementBuilder addStatement(int pos, Statement statement) {
    if (!statements.contains(statement)) {
      statements.add(pos, statement);
    }
    return this;
  }
  public StatementBuilder addStatements(Statement... statements) {
    for (Statement statement : statements) {
      addStatement(statement);
    }
    return this;
  }
  
  public StatementBuilder name(String name) {
    this.name = name;
    return this;
  }

  public StatementBuilder addExport(String key, String value) {
    exports.put(key, value);
    return this;
  }

  public StatementBuilder addExportPerInstance(String instanceId, String key, String value) {
    if (exportsByInstanceId.containsKey(instanceId)) {
      exportsByInstanceId.get(instanceId).put(key, value);
    } else {
      Map<String, String> pairs = Maps.newHashMap();
      pairs.put(key, value);
      exportsByInstanceId.put(instanceId, pairs);
    }
    return this;
  }

  public boolean isEmpty() {
    return statements.size() == 0;
  }

  public Statement build(ClusterSpec clusterSpec) {
    return build(clusterSpec, null);
  }

  /**
   * 
   * @param clusterSpec
   * @param instance
   * @return
   */
  public Statement build(ClusterSpec clusterSpec, Instance instance) {
    if (statements.size() == 0) {
      throw new NoSuchElementException("no statements configured");
    } else {
      InitScript.Builder builder = InitScript.builder();
      builder.name(name);
      builder.exportVariables(new VariablesToExport(exports, exportsByInstanceId, clusterSpec, instance).get());
      builder.run(statements);
      return builder.build();
    }
  }

}
