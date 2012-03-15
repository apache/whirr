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

package org.apache.whirr.service.cdh.integration;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;

import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.hadoop.HadoopProxy;

import org.junit.BeforeClass;

public class CdhYarnServiceTest extends CdhHadoopServiceTest {

  protected static String getPropertiesFilename() {
    return "whirr-hadoop-mr2-cdh-test.properties";
  }
  
  @BeforeClass
  public static void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration(getPropertiesFilename()));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    
    cluster = controller.launchCluster(clusterSpec);
    proxy = new HadoopProxy(clusterSpec, cluster);
    proxy.start();
  }

}
