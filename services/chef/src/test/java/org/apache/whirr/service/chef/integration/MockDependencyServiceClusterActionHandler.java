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

package org.apache.whirr.service.chef.integration;

import java.io.IOException;

import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.chef.Recipe;

/**
 * Test service to test the execution of chef recipes in other services.
 * 
 * @author dralves
 * 
 */
public class MockDependencyServiceClusterActionHandler extends
    ClusterActionHandlerSupport {

  @Override
  public String getRole() {
    return "test-service";
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException,
      InterruptedException {

    Recipe ant = new Recipe("maven");

    addStatement(event, ant);
  }

}
