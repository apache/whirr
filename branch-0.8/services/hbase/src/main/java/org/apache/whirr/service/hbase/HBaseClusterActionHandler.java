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

package org.apache.whirr.service.hbase;

import java.util.Set;
import org.apache.whirr.RolePredicates;
import org.apache.whirr.Cluster.Instance;

import org.apache.whirr.Cluster;
import org.apache.whirr.service.ClusterActionEvent;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionHandlerSupport;

/**
 * Base class for HBase service handlers.
 */
public abstract class HBaseClusterActionHandler
       extends ClusterActionHandlerSupport {

  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with a hbase defaults
   * properties.
   */
  protected synchronized Configuration getConfiguration(
      ClusterSpec clusterSpec) throws IOException {
    return getConfiguration(clusterSpec,
      HBaseConstants.FILE_HBASE_DEFAULT_PROPERTIES);
  }

  protected String getInstallFunction(Configuration config) {
    return getInstallFunction(config, "hbase", HBaseConstants.FUNCTION_INSTALL);
  }

  protected String getConfigureFunction(Configuration config) {
    return getConfigureFunction(config, "hbase", HBaseConstants.FUNCTION_CONFIGURE);
  }

  protected String getMetricsTemplate(ClusterActionEvent event,
      ClusterSpec clusterSpec, Cluster cluster) {
    Configuration conf = clusterSpec.getConfiguration();
    if (conf.containsKey("hbase-metrics.template")) {
      return conf.getString("hbase-metrics.template");
    }
    
    Set<Instance> gmetadInstances = cluster.getInstancesMatching(RolePredicates.role("ganglia-metad"));
    if (!gmetadInstances.isEmpty()) {
      return "hbase-metrics-ganglia.properties.vm";
    }
    
    return "hbase-metrics-null.properties.vm";
  }


}
