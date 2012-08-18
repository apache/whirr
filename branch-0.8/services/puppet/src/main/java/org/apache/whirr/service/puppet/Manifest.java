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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

/**
 * Class representing a single Puppet Module. The module must already be available on the module
 * path.</p>
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
 *  Module nginx = new Module("nginx", null);
 *  addStatement(event, nginx);
 *  ...
 *  OR
 * 
 *  Module ntp = new Module("ntp");
 *  ntp.attribs.put("servers", "[10.0.0.1]" );
 *  addStatement(event,java);
 * 
 *  OR
 * 
 *  Module postgresServer = new Module("postgresql","server");
 *  Module postgresClient = new Module("postgresql","client");
 *  addStatement(event, postgresServer);
 *  addStatement(event, postgresClient);
 * 
 * }
 * 
 * ...
 * </pre>
 * 
 * </blockquote>
 * 
 */
public class Manifest {

  public final String module;
  public final String className;
  public final Map<String, String> attribs = new LinkedHashMap<String, String>();

  @VisibleForTesting
  String fileName = null;

  /**
   * To include the default manifest from a provided module with the specified name. Equivalent to:
   * 
   * puppet apply -e "class { 'module': }"
   * 
   * @param module
   */
  public Manifest(String module) {
    this(module, null);
  }

  /**
   * To run a particular manifest from a provided module with the specified name. Equivalent to:
   * 
   * puppet apply -e "class { 'module::className': )"
   * 
   * @param module
   * @param className
   */
  public Manifest(String module, String className) {
    this.module = module;
    this.className = className;
    fileName = module + (className != null ? "::" + className : "") + "-" + System.currentTimeMillis();
    fileName = fileName.replace(":+", "-"); // replace colons with hyphens so file is easier to
    // deal with
  }

  /**
   * Transforms the Manifest into a puppet resource that can be interpreted by the "puppet apply"
   * command.
   * 
   * @return
   */
  public String toString() {
    StringBuilder resource = new StringBuilder();
    for (String s : toStringList()) {
      if (resource.length() > 0)
        resource.append("\n");
      resource.append(s);
    }
    return resource.toString();
  }

  List<String> toStringList() {
    List<String> result = new ArrayList<String>();

    // First part of the the resource
    // class { 'module::className':
    result.add("class { '" + module + (className != null ? "::" + className : "") + "':");

    // If we have attribs they go in as key => value pairs
    // These go in _unquoted_; user is responsible for supplying the quotes
    for (Map.Entry<String, String> entry : attribs.entrySet()) {
      result.add("  " + entry.getKey() + " => " + entry.getValue() + ",");
    }

    // Close the resource
    result.add("}");

    return result;
  }
}
