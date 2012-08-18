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

package org.apache.whirr.service.solr.integration;

import com.jcraft.jsch.JSchException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.util.BlobCache;
import org.apache.whirr.util.Tarball;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.apache.whirr.RolePredicates.role;

/**
 * Installs the Solr service including configuration. Indexes a document and performs a search.
 */
public class SolrServiceTest {

  private static final Logger LOG = LoggerFactory.getLogger(SolrServiceTest.class);

  public static final String SOLR_ROLE = "solr";
  public static final String SOLR_PORT = "8983";

  private static ClusterSpec clusterSpec;
  private static ClusterController controller;
  private static Cluster cluster;

  @BeforeClass
  public static void beforeClass() throws ConfigurationException, JSchException, IOException, InterruptedException {
    String solrConfigTarballDestination = "target/solrconfig.tar.gz";
    Tarball.createFromDirectory("src/test/resources/conf", solrConfigTarballDestination);
    LOG.info("Created Solr config tarball at "  + solrConfigTarballDestination);

    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("conf") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("conf")));
    }
    config.addConfiguration(new PropertiesConfiguration("whirr-solr-test.properties"));
    clusterSpec = ClusterSpec.withTemporaryKeys(config);
    controller = new ClusterController();

    cluster = controller.launchCluster(clusterSpec);
  }

  @Test
  public void testSolr() throws IOException, SolrServerException {
    Set<Cluster.Instance> instances = cluster.getInstancesMatching(role(SOLR_ROLE));

    for (Cluster.Instance instance : instances) {
      String publicIp = instance.getPublicIp();

      LOG.info("Adding a document to instance " + instance.getId() + " @ " + publicIp);
      
      CommonsHttpSolrServer solrServer = new CommonsHttpSolrServer(String.format("http://%s:%s/solr/core0", instance.getPublicHostName(), SOLR_PORT));

      SolrInputDocument doc = new SolrInputDocument();
      doc.addField("name", "Apache Whirr");
      doc.addField("inceptionYear", "2010");

      solrServer.add(doc);
      solrServer.commit();

      LOG.info("Committed document to instance " + instance.getId() + " @ " + publicIp);

      LOG.info("Performing a search on instance " + instance.getId() + " @ " + publicIp);

      SolrQuery query = new SolrQuery("name:whirr");
      QueryResponse response = solrServer.query(query);
      SolrDocumentList results = response.getResults();
      assertEquals("Search on instance " + instance.getId() + " did NOT return a document!" , 1, results.size());

      SolrDocument resultDoc = results.get(0);
      assertEquals("name field on document of instance " + instance.getId() + " is incorrect", "Apache Whirr", resultDoc.get("name"));
    }
  }

  @AfterClass
  public static void after() throws IOException, InterruptedException {
    if (controller != null) {
      controller.destroyCluster(clusterSpec);
    }

    BlobCache.dropAndCloseAll();
  }
}
