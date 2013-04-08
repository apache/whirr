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

package org.apache.whirr;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.jclouds.byon.Node;

import org.junit.Test;

public class ByonClusterControllerTest {
  @Test
  public void testSpecifyByonCache() throws Exception {
    Map<String,Node> byonNodes = new HashMap<String,Node>();

    byonNodes.put("foo", Node.builder()
                  .id("foo-1234")
                  .name("foo-node")
                  .hostname("1.2.3.4")
                  .osFamily("ubuntu")
                  .osVersion("12.04")
                  .osDescription("foo OS")
                  .description("foo node").build());
    
    ClusterSpec tempSpec = ClusterSpec.withTemporaryKeys();

    tempSpec.setClusterName("test-cluster-for-byon-cache");
    tempSpec.setProvider("byon");
    tempSpec.setServiceName("byon");
    tempSpec.setIdentity("dummy");
    tempSpec.setStateStore("none");
    tempSpec.setByonNodes(byonNodes);
    
    ClusterControllerFactory factory = new ClusterControllerFactory();
    ClusterController controller = factory.create(tempSpec.getServiceName());

    assertThat(controller.getInstances(tempSpec).size(), is(1));
  }


}
