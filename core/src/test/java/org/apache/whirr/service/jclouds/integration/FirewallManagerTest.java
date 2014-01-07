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

package org.apache.whirr.service.jclouds.integration;

import java.io.IOException;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.service.ComputeCache;
import org.apache.whirr.service.FirewallManager;
import org.apache.whirr.service.FirewallManager.Rule;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.domain.Credentials;
import org.jclouds.ec2.EC2Api;
import org.jclouds.ec2.compute.EC2ComputeService;
import org.junit.Before;
import org.junit.Test;

public class FirewallManagerTest {

  private final String region = "us-east-1";

  private ClusterSpec clusterSpec;
  private Set<Cluster.Instance> instances;

  private ComputeServiceContext context;
  private FirewallManager manager;

  private ClusterSpec getTestClusterSpec() throws Exception {
    return ClusterSpec.withTemporaryKeys(
      new PropertiesConfiguration("whirr-core-test.properties"));
  }

  @Before
  public void setUpClass() throws Exception {
    clusterSpec = getTestClusterSpec();
    context =  ComputeCache.INSTANCE.apply(clusterSpec);

    /* create a dummy instance for testing */
    instances = Sets.newHashSet(new Cluster.Instance(
      new Credentials("dummy", "dummy"),
      Sets.newHashSet("dummy-role"),
      "50.0.0.1",
      "10.0.0.1",
      region + "/i-dummy",
      null
    ));

    manager = new FirewallManager(context, clusterSpec, new Cluster(instances));
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testFirewallAuthorizationIsIdempotent() throws IOException {
    if (EC2ComputeService.class.isInstance(context.getComputeService())) {
      EC2Api api = context.unwrapApi(EC2Api.class);

      String groupName = "jclouds#" + clusterSpec.getClusterName();

      api.getSecurityGroupApi().get().createSecurityGroupInRegion(region, groupName, "group description");
      try {
        manager.addRule(
          Rule.create().destination(instances).port(23344)
        );

        /* The second call should not throw an exception. */
        manager.addRule(
          Rule.create().destination(instances).port(23344)
        );

        manager.authorizeAllRules();
      } finally {
        api.getSecurityGroupApi().get()
            .deleteSecurityGroupInRegion(region, groupName);
      }
    }
  }

}
