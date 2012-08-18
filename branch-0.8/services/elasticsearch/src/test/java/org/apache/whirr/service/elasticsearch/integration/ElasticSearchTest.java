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
package org.apache.whirr.service.elasticsearch.integration;

import com.google.common.collect.Iterables;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.elasticsearch.ElasticSearchHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.whirr.RolePredicates.role;

public class ElasticSearchTest {

  private static final Logger LOG =
    LoggerFactory.getLogger(ElasticSearchHandler.class);

  private ClusterSpec clusterSpec;
  private ClusterController controller;
  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    config.addConfiguration(new PropertiesConfiguration("whirr-elasticsearch-test.properties"));
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();
    cluster = controller.launchCluster(clusterSpec);
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testCheckNumberOfNodes() throws Exception {
    for(int i = 0; i<20; i++) {
      int nodes = getNumberOfNodes();
      LOG.info("{}/{} nodes joined the elasticsearch cluster",
        nodes, cluster.getInstances().size());
      if (nodes == cluster.getInstances().size()) {
        return;
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {}
    }
    throw new Exception("All nodes did not joined the cluster as expected");
  }

  private int getNumberOfNodes() throws Exception {
    String healthInfo = getHealthInfo();
    Pattern nodesPattern = Pattern.compile("\".*number_of_nodes\":(\\d+).*");
    Matcher matcher = nodesPattern.matcher(healthInfo);
    if (matcher.find()) {
      return Integer.parseInt(matcher.group(1));
    }
    return 0;
  }

  private String getHealthInfo() throws Exception {
    for(int i=0; i<20; i++) {
      try {
        Cluster.Instance instance = Iterables.get(
          cluster.getInstancesMatching(role(ElasticSearchHandler.ROLE)), 0);
        String address = instance.getPublicAddress().getHostAddress();

        URL url = new URL(String.format("http://%s:9200/_cluster/health", address));
        BufferedReader in = new BufferedReader(new InputStreamReader(url.openStream()));

        StringBuilder builder = new StringBuilder();
        String line;
        while((line = in.readLine()) != null) {
          builder.append(line);
        }
        in.close();
        return builder.toString();

      } catch(IOException e) {
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {}
      }
    }
    throw new Exception("Unable to get cluster health info.");
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    controller.destroyCluster(clusterSpec);
  }

}
