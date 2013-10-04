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
package org.apache.whirr.service.druid.integration;

import com.google.common.collect.Iterables;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.ClusterController;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.druid.DruidRealtimeClusterActionHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;

import static org.apache.whirr.RolePredicates.role;

public class DruidServiceTest {
    private static final Logger LOG =
            LoggerFactory.getLogger(DruidRealtimeClusterActionHandler.class);

    private ClusterSpec clusterSpec;
    private ClusterController controller;
    private Cluster cluster;

    @Before
    public void setUp() throws Exception {
        CompositeConfiguration config = new CompositeConfiguration();
        config.addConfiguration(new PropertiesConfiguration("whirr-druid-test.properties"));
        if (System.getProperty("config") != null) {
            config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
        }
        clusterSpec = ClusterSpec.withTemporaryKeys(config);
        controller = new ClusterController();
        //controller = new ClusterControllerFactory().create(clusterSpec.getServiceName());
        cluster = controller.launchCluster(clusterSpec);
    }

    @Test(timeout = TestConstants.ITEST_TIMEOUT)
    public void testSegmentMetadata() {
        try {
            String response = getQueryInfo();
            return;
        }
        catch(Exception e)
        {
            LOG.debug("Caught exception contacting cluster");
        }
    }

    private String getQueryInfo() throws Exception {
        for(int i=0; i<20; i++) {
            try {
                Cluster.Instance instance = Iterables.get(
                        cluster.getInstancesMatching(role(DruidRealtimeClusterActionHandler.ROLE)), 0);
                String address = instance.getPublicAddress().getHostAddress();
                String port = DruidRealtimeClusterActionHandler.PORT.toString();

                URL url = new URL(String.format("http://%s:%s/druid/v2", address, port));
                String query = "{\n" +
                        "  \"queryType\":\"segmentMetadata\",\n" +
                        "  \"dataSource\":\"sample_datasource\",\n" +
                        "  \"intervals\":[\"2013-01-01/2014-01-01\"],\n" +
                        "}";
                URLConnection conn = url.openConnection();
                conn.setDoOutput(true);

                OutputStreamWriter writer = new OutputStreamWriter(conn.getOutputStream());

                writer.write(query);
                writer.flush();

                String line;
                BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                StringBuilder builder = new StringBuilder();

                while ((line = reader.readLine()) != null) {
                    builder.append(line);
                }
                writer.close();
                reader.close();

                return builder.toString();

            } catch(IOException e) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {}
            }
        }
        throw new Exception("Unable to get cluster metadata info.");
    }

    @After
    public void tearDown() throws IOException, InterruptedException {
        controller.destroyCluster(clusterSpec);
    }
}
