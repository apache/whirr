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

package org.apache.whirr.service;

import com.google.common.collect.ListMultimap;
import com.jcraft.jsch.JSchException;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.jclouds.compute.callables.RunScriptOnNode;
import org.jclouds.compute.callables.RunScriptOnNodeAsInitScriptUsingSsh;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.scriptbuilder.InitBuilder;
import org.junit.Test;

import java.io.IOException;

import static junit.framework.Assert.assertFalse;

public class DryRunModuleTest {

    public static class Noop2ClusterActionHandler extends
            ClusterActionHandlerSupport {

        @Override
        public String getRole() {
            return "noop2";
        }

    }

    public static class Noop3ClusterActionHandler extends
            ClusterActionHandlerSupport {

        @Override
        public String getRole() {
            return "noop3";
        }
    }

    /**
     * Simple test that asserts that a 1 node cluster was launched and bootstrap
     * and configure scripts were executed.
     * 
     * @throws ConfigurationException
     * @throws IOException
     * @throws JSchException
     * @throws InterruptedException
     */
    @Test
    public void testNoInitScriptsAfterConfigurationStarted()
            throws ConfigurationException, JSchException, IOException,
            InterruptedException {

        CompositeConfiguration config = new CompositeConfiguration();
        config.setProperty("whirr.provider", "stub");
        config.setProperty("whirr.cluster-name", "stub-test");
        config.setProperty("whirr.instance-templates",
                "10 noop+noop3,10 noop2+noop,10 noop3+noop2");

        ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(config);
        ClusterController controller = new ClusterController();

        controller.launchCluster(clusterSpec);

        DryRun dryRun = DryRunModule.getDryRun();
        ListMultimap<NodeMetadata, RunScriptOnNode> list = dryRun
                .getExecutions();

        // this tests the barrier by making sure that once a configure
        // script is executed no more setup scripts are executed.

        boolean configStarted = false;
        for (RunScriptOnNode script : list.values()) {
            if (!configStarted && getScriptName(script).startsWith("configure")) {
                configStarted = true;
                continue;
            }
            if (configStarted) {
                assertFalse(
                  "A setup script was executed after the first configure script.",
                  getScriptName(script).startsWith("setup"));
            }
        }
    }

    private String getScriptName(RunScriptOnNode script) {
        return ((InitBuilder) ((RunScriptOnNodeAsInitScriptUsingSsh) script)
                .getStatement()).getInstanceName();
    }

}
