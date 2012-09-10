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

import java.util.LinkedList;
import java.util.List;

public class TemplateCompleter implements Completer {

 private RoleCompleter roleCompleter;

  @Override
  public int complete(String buffer, int cursor, List<String> candidates) {
    if (buffer == null || !(buffer.contains("[") || buffer.contains("+") || buffer.contains(",") )) {
      return roleCompleter.complete(buffer, cursor, candidates);
    } else {
      int lastNumRoleDelimeter = buffer.lastIndexOf(",") + 1;
      int lastRoleDelimeter = buffer.lastIndexOf("+") + 1;
      int roleOpener = buffer.lastIndexOf("[") + 1;

      int pivot = Math.max(Math.max(lastNumRoleDelimeter, lastRoleDelimeter), roleOpener);
      int result = roleCompleter.complete(buffer.substring(pivot), cursor, candidates);
      List<String> updatedCandidates = new LinkedList<String>();
      for (String candidate : candidates) {
        candidate = buffer.substring(0, pivot) + candidate;
        updatedCandidates.add(candidate);
      }
      candidates.clear();
      for (String candidate : updatedCandidates) {
        candidates.add(candidate);
      }
      return result;
    }
  }

  public RoleCompleter getRoleCompleter() {
    return roleCompleter;
  }

  public void setRoleCompleter(RoleCompleter roleCompleter) {
    this.roleCompleter = roleCompleter;
  }
}
