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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A factory for ClusterStateStores.
 * 
 */
public class ClusterStateStoreFactory {

  private static final Logger LOG = LoggerFactory
    .getLogger(ClusterStateStoreFactory.class);

  private class NoopClusterStateStore extends ClusterStateStore {
    public NoopClusterStateStore() {
      LOG.warn("No cluster state is going to be persisted. There is no easy " +
        "way to retrieve instance roles after launch.");
    }
    @Override
    public Cluster load() throws IOException {
      return null;
    }
    @Override
    public void save(Cluster cluster) throws IOException {
    }
    @Override
    public void destroy() throws IOException {
    }
  }

  public ClusterStateStore create(ClusterSpec spec) {
    return create(spec, new PropertiesConfiguration());
  }

  public ClusterStateStore create(ClusterSpec spec, Configuration conf) {
    if ("local".equals(spec.getStateStore())) {
      return new FileClusterStateStore(spec);

    } else if("blob".equals(spec.getStateStore())) {
      return new BlobClusterStateStore(spec);

    } else {
      return new NoopClusterStateStore();
    }
  }

}
