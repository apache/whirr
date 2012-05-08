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


import org.junit.Test;
import org.junit.runner.RunWith;
import org.openengsb.labs.paxexam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.ExamReactorStrategy;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.spi.reactors.AllConfinedStagedReactorFactory;

import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.logLevel;
import static org.ops4j.pax.exam.CoreOptions.scanFeatures;

@RunWith(JUnit4TestRunner.class)
@ExamReactorStrategy(AllConfinedStagedReactorFactory.class)
public class WhirrServicesTest extends WhirrKarafTestSupport {

  @Test
  public void testServices() throws InterruptedException {
    //Install all services
    executeCommand("features:install whirr-cassandra");
    executeCommand("features:install whirr-chef");
    executeCommand("features:install whirr-elasticsearch");
    executeCommand("features:install whirr-ganglia");
    executeCommand("features:install whirr-hadoop");
    executeCommand("features:install whirr-hama");
    executeCommand("features:install whirr-hbase");
    executeCommand("features:install whirr-puppet");
    executeCommand("features:install whirr-pig");
    executeCommand("features:install whirr-mahout");
    executeCommand("features:install whirr-zookeeper");

    System.err.println(executeCommand("osgi:list"));

    //Test that services properly register to OSGi service registry.
    getOsgiService("org.apache.whirr.ClusterController", "(name=default)", SERVICE_TIMEOUT);

    testService("cassandra");
    testService("chef");
    testService("elasticsearch");
    testService("ganglia-monitor", "ganglia-metad");
    testService("hadoop-namenode", "hadoop-datanode", "hadoop-jobtracker", "hadoop-tasktracker");
    testService("hama-master", "hama-groomserver");
    testService("hbase-master", "hbase-regionserver", "hbase-restserver", "hbase-avroserver", "hbase-thriftserver");
    testService("puppet-install");
    testService("pig-client");
    testService("mahout-client");
    testService("zookeeper");
  }


  /**
   * Tests that the ClusterActionHandler service has been properly exported.
   *
   * @param roleNames the name of the roles to retrieve
   */
  public void testService(String... roleNames) {
    for (String roleName : roleNames) {
      getOsgiService("org.apache.whirr.service.ClusterActionHandler",
          String.format("(name=%s)", roleName), SERVICE_TIMEOUT);
    }
  }


  @Configuration
  public Option[] config() {
    return new Option[]{
        whirrDistributionConfiguration(), keepRuntimeFolder(), logLevel(LogLevelOption.LogLevel.ERROR),
        scanFeatures(String.format(WHIRR_FEATURE_URL, MavenUtils
            .getArtifactVersion(WHIRR_KARAF_GROUP_ID, WHIRR_KARAF_ARTIFACT_ID)), "whirr").start()
    };
  }
}
