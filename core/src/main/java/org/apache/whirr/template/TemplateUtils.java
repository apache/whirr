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

package org.apache.whirr.template;

import java.util.Properties;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.log.Log4JLogChute;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;

import java.io.StringWriter;
import java.util.Arrays;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;

public class TemplateUtils {

  public static Statement createFileFromTemplate(String path, VelocityEngine engine, String templateName, ClusterSpec clusterSpec, Cluster cluster) {
    return Statements.createOrOverwriteFile(path, Arrays.asList(processTemplate(engine, templateName, clusterSpec, cluster).split("\n")));
  }

  public static String processTemplate(VelocityEngine engine, String templateName, ClusterSpec clusterSpec, Cluster cluster) {
    VelocityContext context = new VelocityContext();
    context.put("clusterSpec", clusterSpec);
    context.put("cluster", cluster);
    context.put("RolePredicates", RolePredicates.class);

    Template template = engine.getTemplate(templateName);
    StringWriter writer = new StringWriter();
    
    template.merge(context, writer);
    
    return writer.toString();
  }
  
  public static VelocityEngine newVelocityEngine() {
    Properties defaults = new Properties();
    defaults.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath"); 
    defaults.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
    defaults.setProperty("runtime.log.logsystem.class", Log4JLogChute.class.getName());
    defaults.setProperty("runtime.log.logsystem.log4j.logger", "org.apache.velocity");
    return newVelocityEngine(defaults);
  }
  
  public static VelocityEngine newVelocityEngine(Properties properties) {
    return new VelocityEngine(properties);
  }


}
