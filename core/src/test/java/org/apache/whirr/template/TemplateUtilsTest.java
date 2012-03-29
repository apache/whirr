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

import com.google.common.collect.Sets;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.velocity.app.VelocityEngine;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.jclouds.domain.Credentials;
import org.junit.Assert;
import org.junit.Test;


public class TemplateUtilsTest {

  @Test
  public void testProcessTemplate() throws Exception {
    Credentials credentials = new Credentials("dummy", "dummy");
    Cluster.Instance instance = new Cluster.Instance(credentials,
        Sets.newHashSet("foo"), "127.0.0.1", "127.0.0.1", "id-0", null);

    ClusterSpec clusterSpec = new ClusterSpec(new PropertiesConfiguration("whirr-core-test.properties"));
    Cluster cluster = new Cluster(Sets.newHashSet(instance));
    VelocityEngine ve = TemplateUtils.newVelocityEngine();

    String result = TemplateUtils.processTemplate(ve, "template-test.txt.vm", clusterSpec, cluster);
    
    Assert.assertEquals("instance ip: 127.0.0.1", result);
  }
  
}
