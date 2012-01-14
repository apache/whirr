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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.jclouds.scriptbuilder.ScriptBuilder;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.jclouds.scriptbuilder.domain.Statements.exec;

public class StatementBuilder {

  private static final Logger LOG =
    LoggerFactory.getLogger(StatementBuilder.class);

  class EmptyStatement implements Statement {

    @Override
    public Iterable<String> functionDependencies(OsFamily osFamily) {
      return ImmutableSet.of();
    }

    @Override
    public String render(OsFamily osFamily) {
      return "";
    }
  }

  class ConsolidatedStatement implements Statement {

    private ClusterSpec clusterSpec;
    private Instance instance;

    public ConsolidatedStatement(ClusterSpec clusterSpec, Instance instance) {
      this.clusterSpec = clusterSpec;
      this.instance = instance;
    }

    @Override
    public Iterable<String> functionDependencies(OsFamily family) {
      List<String> functions = Lists.newArrayList();
      for (Statement statement : statements) {
        Iterables.addAll(functions, statement.functionDependencies(family));
      }
      return functions;
    }

    @Override
    public String render(OsFamily family) {
      ScriptBuilder scriptBuilder = new ScriptBuilder();
      Map<String, String> metadataMap = Maps.newLinkedHashMap();

      addEnvironmentVariablesFromClusterSpec(metadataMap);
      addDefaultEnvironmentVariablesForInstance(metadataMap, instance);
      metadataMap.putAll(exports);
      addPerInstanceCustomEnvironmentVariables(metadataMap, instance);

      String writeVariableExporters = Utils.writeVariableExporters(metadataMap, family);
      scriptBuilder.addStatement(exec(writeVariableExporters));

      for (Statement statement : statements) {
        scriptBuilder.addStatement(statement);
      }

      return scriptBuilder.render(family);
    }

    private void addPerInstanceCustomEnvironmentVariables(Map<String, String> metadataMap, Instance instance) {
      if (instance != null && exportsByInstanceId.containsKey(instance.getId())) {
        metadataMap.putAll(exportsByInstanceId.get(instance.getId()));
      }
    }

    private void addDefaultEnvironmentVariablesForInstance(Map<String, String> metadataMap, Instance instance) {
      metadataMap.putAll(
        ImmutableMap.of(
          "clusterName", clusterSpec.getClusterName(),
          "cloudProvider", clusterSpec.getProvider()
        )
      );
      if (instance != null) {
        metadataMap.putAll(
          ImmutableMap.of(
            "roles", Joiner.on(",").join(instance.getRoles()),
            "publicIp", instance.getPublicIp(),
            "privateIp", instance.getPrivateIp()
          )
        );
        if (!clusterSpec.isStub()) {
          try {
            metadataMap.putAll(
              ImmutableMap.of(
                "publicHostName", instance.getPublicHostName(),
                "privateHostName", instance.getPrivateHostName()
              )
            );
          } catch (IOException e) {
            LOG.warn("Could not resolve hostname for " + instance, e);
          }
        }
      }
    }

    private void addEnvironmentVariablesFromClusterSpec(Map<String, String> metadataMap) {
      for (Iterator<?> it = clusterSpec.getConfiguration().getKeys("whirr.env"); it.hasNext(); ) {
        String key = (String) it.next();
        String value = clusterSpec.getConfiguration().getString(key);
        metadataMap.put(key.substring("whirr.env.".length()), value);
      }
    }
  }

  protected List<Statement> statements = Lists.newArrayList();
  protected Map<String, String> exports = Maps.newLinkedHashMap();
  protected Map<String, Map<String, String>> exportsByInstanceId = Maps.newHashMap();

  public void addStatement(Statement statement) {
    if (!statements.contains(statement)) {
      statements.add(statement);
    }
  }

  public void addStatements(Statement... statements) {
    for (Statement statement : statements) {
      addStatement(statement);
    }
  }

  public void addExport(String key, String value) {
    exports.put(key, value);
  }

  public void addExportPerInstance(String instanceId, String key, String value) {
    if (exportsByInstanceId.containsKey(instanceId)) {
      exportsByInstanceId.get(instanceId).put(key, value);
    } else {
      Map<String, String> pairs = Maps.newHashMap();
      pairs.put(key, value);
      exportsByInstanceId.put(instanceId, pairs);
    }
  }

  public boolean isEmpty() {
    return statements.size() == 0;
  }

  public Statement build(ClusterSpec clusterSpec) {
    return build(clusterSpec, null);
  }

  public Statement build(ClusterSpec clusterSpec, Instance instance) {
    if (statements.size() == 0) {
      return new EmptyStatement();
    } else {
      return new ConsolidatedStatement(clusterSpec, instance);
    }
  }

}
