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

package org.apache.whirr.karaf.itest.integration;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.openengsb.labs.paxexam.karaf.options.LogLevelOption;
import org.ops4j.pax.exam.MavenUtils;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.Configuration;
import org.ops4j.pax.exam.junit.ExamReactorStrategy;
import org.ops4j.pax.exam.junit.JUnit4TestRunner;
import org.ops4j.pax.exam.spi.reactors.AllConfinedStagedReactorFactory;

import java.io.IOException;

import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.keepRuntimeFolder;
import static org.openengsb.labs.paxexam.karaf.options.KarafDistributionOption.logLevel;
import static org.ops4j.pax.exam.CoreOptions.scanFeatures;

@RunWith(JUnit4TestRunner.class)
@ExamReactorStrategy(AllConfinedStagedReactorFactory.class)
public class WhirrKarafZookeeperServiceTest extends WhirrLiveTestSupport {

  @Before
  public void setUp() {
    lookupConfigurationAdmin();
  }

  @After
  public void tearDown() {
    if (isLiveConfigured()) {
      releaseConfigurationAdmin();
      System.err.println(executeCommand("whirr:destroy-cluster --pid " + ZOOKEEPER_RECIPE_PID, 10 * 60 * 1000L, false));
    }
  }

  @Test
  public void testLive() throws IOException, ConfigurationException {
    if (isLiveConfigured()) {
      loadConfiguration("whirr-zookeeper-test.properties", ZOOKEEPER_RECIPE_PID);

      System.err.println(executeCommands("config:edit " + ZOOKEEPER_RECIPE_PID, "config:proplist", "config:cancel"));
      System.err.println(executeCommand("whirr:launch-cluster --pid " + ZOOKEEPER_RECIPE_PID, 20 * 60 * 1000L, false));

      // TODO: connect to the launched cluster to make sure it works (we need a karaf
      // aware cluster state store to be able to retrieve the cluster IP addresses)

    } else {
      System.err.println("Live test not properly configured, please add whirr.test.provider, " +
          "whirr.test.identity & whirr.test.credential as system properties.\n" +
          "Also note that currently only aws-ec2, clouservers-us, cloudservers-uk are the supported providers " +
          "for this integration test.");
    }
  }

  @Configuration
  public Option[] config() {
    return new Option[]{
        whirrDistributionConfiguration(), keepRuntimeFolder(), logLevel(LogLevelOption.LogLevel.ERROR),
        systemProperty("whirr.test.provider"),
        systemProperty("whirr.test.identity"),
        systemProperty("whirr.test.credential"),
        scanFeatures(String.format(WHIRR_FEATURE_URL,
            MavenUtils.getArtifactVersion(WHIRR_KARAF_GROUP_ID, WHIRR_KARAF_ARTIFACT_ID)),
            "jclouds-aws-ec2", "jclouds-cloudserver-us", "jclouds-cloudserver-uk", "whirr-zookeeper").start()
    };
  }
}
