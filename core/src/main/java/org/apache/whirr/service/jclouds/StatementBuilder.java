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
import java.util.List;
import java.util.Map;

import static org.jclouds.scriptbuilder.domain.Statements.exec;

public class StatementBuilder {
  
  private static final Logger LOG =
    LoggerFactory.getLogger(StatementBuilder.class);
  
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
      Map<String, String> metadataMap = Maps.newHashMap();
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
      // Write export statements out directly
      // Using InitBuilder would be a possible improvement
      String writeVariableExporters = Utils.writeVariableExporters(metadataMap, family);
      scriptBuilder.addStatement(exec(writeVariableExporters));
      for (Statement statement : statements) {
        scriptBuilder.addStatement(statement);
      }

      // Quick fix: jclouds considers that a script that runs for <2 seconds failed
      scriptBuilder.addStatement(exec("sleep 4"));

      return scriptBuilder.render(family);
    }
  }
  
  protected List<Statement> statements = Lists.newArrayList();
  
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

  public Statement build(ClusterSpec clusterSpec) {
    return build(clusterSpec, null);
  }

  public Statement build(ClusterSpec clusterSpec, Instance instance) {
    return new ConsolidatedStatement(clusterSpec, instance);
  }

}
