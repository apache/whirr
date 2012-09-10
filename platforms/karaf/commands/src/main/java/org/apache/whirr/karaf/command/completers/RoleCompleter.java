/*
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
package org.apache.whirr.karaf.command.completers;

import org.apache.karaf.shell.console.Completer;
import org.apache.karaf.shell.console.completer.StringsCompleter;
import org.apache.whirr.service.ClusterActionHandler;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class RoleCompleter implements Completer {

  protected final StringsCompleter delegate = new StringsCompleter();
  private List<ClusterActionHandler> clusterActionHandlers;

  @Override
  public int complete(String buffer, int cursor, List<String> candidates) {
    delegate.getStrings().clear();
    for (String role : getRoles()) {
      delegate.getStrings().add(role);
    }
    return delegate.complete(buffer, cursor, candidates);
  }

  public Set<String> getRoles() {
    Set<String> roles = new LinkedHashSet();
    if (clusterActionHandlers != null && !clusterActionHandlers.isEmpty()) {
      for (ClusterActionHandler clusterActionHandler : clusterActionHandlers) {
        roles.add(clusterActionHandler.getRole());
      }
    }
    return roles;
  }

  public List<ClusterActionHandler> getClusterActionHandlers() {
    return clusterActionHandlers;
  }

  public void setClusterActionHandlers(List<ClusterActionHandler> clusterActionHandlers) {
    this.clusterActionHandlers = clusterActionHandlers;
  }
}
