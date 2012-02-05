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

package org.apache.whirr.karaf.itest;


import org.apache.whirr.ClusterController;
import org.apache.whirr.service.ClusterActionHandler;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openengsb.labs.paxexam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.ExamReactorStrategy;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.spi.reactors.AllConfinedStagedReactorFactory;

import java.util.Map;

import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.logLevel;

@RunWith(JUnit4TestRunner.class)
@ExamReactorStrategy(AllConfinedStagedReactorFactory.class)
public class WhirrServicesTest extends WhirrKarafTestSupport {

  @Test
  public void testServices() throws InterruptedException {
    //Install Whirr
    installWhirr();
    System.err.println(executeCommand("osgi:list"));
    //Install all services
    executeCommand("features:install whirr-cassandra");
    executeCommand("features:install whirr-chef");
    executeCommand("features:install whirr-elasticsearch");
    executeCommand("features:install whirr-ganglia");
    executeCommand("features:install whirr-hadoop");
    executeCommand("features:install whirr-hama");
    executeCommand("features:install whirr-hbase");
    executeCommand("features:install whirr-puppet");
    executeCommand("features:install whirr-mahout");
    //executeCommand("features:install whirr-voldemort");
    executeCommand("features:install whirr-zookeeper");

    System.err.println(executeCommand("osgi:list"));

    //Test that services properly register to OSGi service registry.
    ClusterController clusterController = getOsgiService(ClusterController.class, "(name=default)", SERVICE_TIMEOUT);
    Map<String, ClusterActionHandler> actionHandlerMap = clusterController.getHandlerMapFactory().create();
    Assert.assertNotNull(clusterController);

    testService(actionHandlerMap, "cassandra");
    testService(actionHandlerMap, "chef");
    testService(actionHandlerMap, "elasticsearch");
    testService(actionHandlerMap, "ganglia-monitor", "ganglia-metad");
    testService(actionHandlerMap, "hadoop-namenode", "hadoop-datanode", "hadoop-jobtracker", "hadoop-tasktracker");
    testService(actionHandlerMap, "hama-master", "hama-groomserver");
    testService(actionHandlerMap, "hbase-master", "hbase-regionserver", "hbase-restserver", "hbase-avroserver", "hbase-thriftserver");
    testService(actionHandlerMap, "puppet-install");
    testService(actionHandlerMap, "mahout-client");
    //testService(actionHandlerMap,"voldemort");
    testService(actionHandlerMap, "zookeeper");
  }


  /**
   * Tests that the {@link ClusterActionHandler} service has been properly exported.
   *
   * @param roleNames
   */
  public void testService(Map actionHandlerMap, String... roleNames) throws InterruptedException {
    for (String roleName : roleNames) {
      ClusterActionHandler clusterActionHandler = getOsgiService(ClusterActionHandler.class,
        String.format("(name=%s)", roleName), SERVICE_TIMEOUT);
      Assert.assertNotNull(clusterActionHandler);
      Assert.assertEquals(clusterActionHandler.getRole(), roleName);
      Assert.assertTrue(actionHandlerMap.containsKey(roleName));
    }
  }


  @Configuration
  public Option[] config() {
    return new Option[]{
      whirrDistributionConfiguration(), keepRuntimeFolder(), logLevel(LogLevelOption.LogLevel.ERROR)};
  }
}
