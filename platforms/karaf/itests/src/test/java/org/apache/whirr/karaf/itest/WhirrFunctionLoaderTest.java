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


import junit.framework.Assert;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.functionloader.osgi.ServiceFunctionLoader;
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
public class WhirrFunctionLoaderTest extends WhirrKarafTestSupport {

  @Test
  public void testServices() throws InterruptedException {
    ServiceFunctionLoader loader = new ServiceFunctionLoader(bundleContext);

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
    executeCommand("features:install whirr-solr");
    executeCommand("features:install whirr-mahout");
    executeCommand("features:install whirr-yarn");
    executeCommand("features:install whirr-zookeeper");

    System.err.println(executeCommand("osgi:list"));

    //Test that services properly register to OSGi service registry.
    getOsgiService("org.apache.whirr.ClusterController", "(name=default)", SERVICE_TIMEOUT);

    //Check Cassandra Functions
    String function = loader.loadFunction("cleanup_cassandra", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("configure_cassandra", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_cassandra", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("start_cassandra", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("stop_cassandra", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Chef Functions
    function = loader.loadFunction("install_chef", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Elastic Search Functions
    function = loader.loadFunction("cleanup_elasticsearch", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("configure_elasticsearch", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_elasticsearch", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("start_elasticsearch", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("stop_elasticsearch", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Ganglia Functions
    function = loader.loadFunction("configure_ganglia", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_ganglia", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Hadoop Functions
    function = loader.loadFunction("configure_hadoop", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_hadoop", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Hama Functions
    function = loader.loadFunction("configure_hama", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_hama", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("start_hama", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check HBase Functions
    function = loader.loadFunction("configure_hbase", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_hbase", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Mahout Functions
    function = loader.loadFunction("configure_mahout_client", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Pig Functions
    function = loader.loadFunction("configure_pig_client", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Puppet Functions
    function = loader.loadFunction("install_puppet", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Solr Functions
    function = loader.loadFunction("configure_solr", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_solr", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("start_solr", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("stop_solr", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Yarn Functions
    function = loader.loadFunction("configure_hadoop_mr2", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_hadoop_mr2", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("start_mr_jobhistory", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("start_yarn", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("configure_yarn", OsFamily.UNIX);
    Assert.assertNotNull(function);

    //Check Zookeeper Functions
    function = loader.loadFunction("cleanup_zookeeper", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("configure_zookeeper", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("install_zookeeper", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("start_zookeeper", OsFamily.UNIX);
    Assert.assertNotNull(function);
    function = loader.loadFunction("stop_zookeeper", OsFamily.UNIX);
    Assert.assertNotNull(function);
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
