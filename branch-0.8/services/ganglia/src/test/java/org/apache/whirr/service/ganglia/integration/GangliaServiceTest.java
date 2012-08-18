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

package org.apache.whirr.service.ganglia.integration;

import com.google.common.collect.Sets;
import junit.framework.TestCase;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.ganglia.GangliaMetadClusterActionHandler;
import org.apache.whirr.service.ganglia.GangliaMonitorClusterActionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.Socket;

import static org.apache.whirr.RolePredicates.anyRoleIn;
import static org.apache.whirr.service.ganglia.GangliaMetadClusterActionHandler.GANGLIA_METAD_ROLE;
import static org.apache.whirr.service.ganglia.GangliaMonitorClusterActionHandler.GANGLIA_MONITOR_ROLE;

public class GangliaServiceTest extends TestCase {
  
  private ClusterSpec clusterSpec;
  private Cluster cluster;
  private ClusterController controller;
  
  @Before
  public void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-ganglia-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterControllerFactory().create(clusterSpec.getServiceName());
    
    cluster = controller.launchCluster(clusterSpec);
  }
  
  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void test() throws Exception {
    Instance metad = cluster.getInstanceMatching(RolePredicates.role(GangliaMetadClusterActionHandler.GANGLIA_METAD_ROLE));
    String metadHostname = metad.getPublicHostName();

    assertNotNull(metadHostname);
    HttpClient client = new HttpClient();
    GetMethod getIndex = new GetMethod(String.format("http://%s/ganglia/", metadHostname));
    int statusCode = client.executeMethod(getIndex);

    assertEquals("Status code should be 200", HttpStatus.SC_OK, statusCode);
    String indexPageHTML = getIndex.getResponseBodyAsString();
    assertTrue("The string 'Ganglia' should appear on the index page", indexPageHTML.contains("Ganglia"));    
    assertTrue("The string 'WhirrGrid' should appear on the index page", indexPageHTML.contains("WhirrGrid"));    

    // Now check the xml produced when connecting to the ganglia monitor port on all instances.
    for (Instance instance: cluster.getInstancesMatching(anyRoleIn(Sets.<String>newHashSet(GANGLIA_METAD_ROLE, GANGLIA_MONITOR_ROLE)))) {
      testMonitorResponse(instance);
    }
  }
  
  private void testMonitorResponse(Instance instance) throws IOException {
    Socket s = null;
    try {
      s = new Socket(instance.getPublicAddress(), GangliaMonitorClusterActionHandler.GANGLIA_MONITOR_PORT);
      String gangliaXml = IOUtils.toString(s.getInputStream());
      assertTrue(String.format("The ganglia monitor output on instance %s with roles %s did not contain 'HOST NAME'", 
              instance.getPublicHostName(), instance.getRoles()), gangliaXml.contains("HOST NAME"));
    } finally {
      if (s != null) s.close();
    }    
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    controller.destroyCluster(clusterSpec);
  }
  
}
