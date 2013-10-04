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
package org.apache.whirr.service.druid;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;

import org.apache.whirr.Cluster;
import org.apache.whirr.service.ClusterActionEvent;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.zookeeper.ZooKeeperCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

public abstract class DruidClusterActionHandler
        extends ClusterActionHandlerSupport {

    private static final Logger LOG =
            LoggerFactory.getLogger(DruidClusterActionHandler.class);

    public abstract String getRole();
    public abstract Integer getPort();

    private String readFile( String file ) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String         line = null;
        StringBuilder  stringBuilder = new StringBuilder();
        String         ls = System.getProperty("line.separator");

        while((line = reader.readLine()) != null) {
            stringBuilder.append(line);
            stringBuilder.append(ls);
        }

        return stringBuilder.toString();
    }

    // Always over-ridden in subclass
    @Override
    protected void beforeConfigure(ClusterActionEvent event)
            throws IOException {
        ClusterSpec clusterSpec = event.getClusterSpec();
        Cluster cluster = event.getCluster();
        Configuration conf = getConfiguration(clusterSpec);

        LOG.info("Role: [" + getRole() + "] Port: [" + getPort() + "]");

        // Open a port for the service
        event.getFirewallManager().addRule(
                FirewallManager.Rule.create().destination(role(getRole())).port(getPort())
        );

        handleFirewallRules(event);

        // Zookeeper quorum
        String quorum = ZooKeeperCluster.getHosts(cluster, true);
        LOG.info("ZookeeperCluster.getHosts(cluster): " + quorum);

        // Get MySQL Server address
        String mysqlAddress = DruidCluster.getMySQLPublicAddress(cluster);
        LOG.info("DruidCluster.getMySQLPublicAddress(cluster).getHostAddress(): " + mysqlAddress);

        // Get Blobstore and bucket
        Map<String, String> env = System.getenv();
        String identity = clusterSpec.getBlobStoreIdentity();
        String credential = clusterSpec.getBlobStoreCredential();
        String s3Bucket = conf.getString("whirr.druid.pusher.s3.bucket");
        LOG.info("whirr.druid.pusher.s3.bucket: " + s3Bucket);

        addStatement(event, call("retry_helpers"));
        addStatement(event, call("configure_hostnames"));
        addStatement(event, call("configure_druid",
                getRole(),
                quorum,
                getPort().toString(),
                mysqlAddress,
                identity,
                credential,
                s3Bucket
        ));

        // Configure the realtime spec for realtime nodes
        if(getRole().equals("druid-realtime")) {
            String specPath = (String)conf.getProperty("whirr.druid.realtime.spec.path");
            LOG.info("whirr.druid.realtime.spec.path" + specPath);

            if(specPath == null || specPath.equals("")) {
                // Default to the included realtime.spec
                specPath = DruidClusterActionHandler.class.getResource("/" + "realtime.spec").getPath();
                // prepareRemoteFileUrl(event, specPath);
            }

            // Quorum is a variable in the realtime.spec
            String realtimeSpec = "'" + readFile(specPath) + "'";
            addStatement(event, call("configure_realtime", quorum, realtimeSpec));
        }
    }

    @Override
    protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
        ClusterSpec clusterSpec = event.getClusterSpec();
        Configuration conf = getConfiguration(clusterSpec);

        addStatement(event, call("install_openjdk"));
        addStatement(event, call("retry_helpers"));
        addStatement(event, call("configure_hostnames"));

        //addStatement(event, call(getInstallFunction(conf, "java", "install_oracle_jdk7")));

        // Get the right version of druid and map this to the tarUrl
        String druidVersion = (String)conf.getProperty("whirr.druid.version");
        LOG.info("whirr.druid.version: " + druidVersion);
        String tarUrl = DruidCluster.getDownloadUrl(druidVersion);
        LOG.info("whirr tarUrl: " + tarUrl);

        addStatement(event, call("install_druid", tarUrl));
    }

    protected synchronized Configuration getConfiguration(ClusterSpec clusterSpec) throws IOException {
        return getConfiguration(clusterSpec, "whirr-druid-default.properties");
    }

    protected String getConfigureFunction(Configuration config) {
        return "configure_druid";
    }

    @Override
    protected void beforeStart(ClusterActionEvent event) throws IOException {
        Configuration config = getConfiguration(event.getClusterSpec());
        String configureFunction = getConfigureFunction(config);

        if (configureFunction.equals("configure_druid")) {
            addStatement(event, call(getStartFunction(config), getRole()));
        } else {
        }
    }

    @Override
    protected void beforeStop(ClusterActionEvent event) throws IOException {
        addStatement(event, call("stop_druid"));
    }

    @Override
    protected void beforeCleanup(ClusterActionEvent event) throws IOException {
        addStatement(event, call("cleanup_druid"));
    }

    protected String getStartFunction(Configuration config) {
        return getStartFunction(config, getRole(), "start_druid");
    }


}
