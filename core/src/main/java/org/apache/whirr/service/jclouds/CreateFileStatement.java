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

import java.util.Collections;
import java.util.List;

import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;

public class CreateFileStatement implements Statement {
  
  private String path;
  private List<String> lines;
  
  public CreateFileStatement(String path, List<String> lines) {
     this.path = path;
     this.lines = lines;
  }

  @Override
  public Iterable<String> functionDependencies(OsFamily osFamily) {
    return Collections.emptyList();
  }

  @Override
  public String render(OsFamily osFamily) {
    StringBuilder builder = new StringBuilder();
    builder.append(Statements.rm(path).render(osFamily));
    builder.append(Statements.appendFile(path, lines).render(osFamily));
    
    return builder.toString();
  }

}
