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

package org.apache.whirr.state.integration;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.TestConstants;
import org.apache.whirr.state.BlobClusterStateStore;
import org.apache.whirr.service.BlobStoreContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.domain.Credentials;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class BlobClusterStateStoreTest {

  private static final Logger LOG = LoggerFactory
      .getLogger(BlobClusterStateStoreTest.class);

  private Configuration getTestConfiguration() throws ConfigurationException {
    return new PropertiesConfiguration("whirr-core-test.properties");
  }

  private ClusterSpec getTestClusterSpec() throws Exception {
    return ClusterSpec.withTemporaryKeys(getTestConfiguration());
  }

  @Test(timeout = TestConstants.ITEST_TIMEOUT)
  public void testStoreAndLoadState() throws Exception {
    ClusterSpec spec = getTestClusterSpec();

    BlobStoreContext context = BlobStoreContextBuilder.build(spec);
    String container = generateRandomContainerName(context);
    try {
      spec.setStateStore("blob");
      spec.setStateStoreContainer(container);

      Cluster expected = createTestCluster(new String[]{"region/id1", "region/id2"},
          new String[]{"role1", "role2"});

      BlobClusterStateStore store = new BlobClusterStateStore(spec);
      store.save(expected);

      /* load and check the stored state */
      Cluster stored = store.load();
      Cluster.Instance first = Iterables.getFirst(stored.getInstances(), null);
      assertNotNull(first);

      assertThat(first.getId(), is("region/id1"));
      assertThat(first.getRoles().contains("role1"), is(true));
      assertThat(stored.getInstances().size(), is(2));

      /* destroy stored state and check it no longer exists */
      store.destroy();
      expected = store.load();
      assertNull(expected);

    } finally {
      LOG.info("Removing temporary container '{}'", container);
      context.getBlobStore().deleteContainer(container);
    }
  }

  @Test(expected = IllegalArgumentException.class, timeout = TestConstants.ITEST_TIMEOUT)
  public void testInvalidContainerName() throws Exception {
    ClusterSpec spec = getTestClusterSpec();

    /* underscores are not allowed and it should throw exception */
    spec.setStateStoreContainer("whirr_test");
  }

  private String generateRandomContainerName(BlobStoreContext context) {
    String candidate;
    do {
      candidate = RandomStringUtils.randomAlphanumeric(12).toLowerCase();
    } while(!context.getBlobStore().createContainerInLocation(null, candidate));
    LOG.info("Created temporary container '{}'", candidate);
    return candidate;
  }

  private Cluster createTestCluster(String[] ids, String[] roles) {
    checkArgument(ids.length == roles.length, "each ID should have a role");

    Credentials credentials = new Credentials("dummy", "dummy");
    Set<Cluster.Instance> instances = Sets.newLinkedHashSet();

    for(int i = 0; i < ids.length; i++) {
      String ip = "127.0.0." + (i + 1);
      instances.add(new Cluster.Instance(credentials,
        Sets.newHashSet(roles[i]), ip, ip, ids[i], null));
    }

    return new Cluster(instances);
  }

}
