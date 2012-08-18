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

package org.apache.whirr.service.ganglia;

import org.apache.whirr.Cluster;
import org.apache.whirr.RolePredicates;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import static org.apache.whirr.service.ganglia.GangliaMetadClusterActionHandler.GANGLIA_METAD_ROLE;
import static org.apache.whirr.service.ganglia.GangliaMonitorClusterActionHandler.GANGLIA_MONITOR_ROLE;

public class GangliaCluster {

  public static final String INSTALL_FUNCTION = "install_ganglia";
  public static final String CONFIGURE_FUNCTION = "configure_ganglia";

  public static String getHosts(Cluster cluster) {
    return Joiner.on(',').join(
      GangliaMonitorClusterActionHandler.getHosts(cluster.getInstancesMatching(
        RolePredicates.anyRoleIn(
          ImmutableSet.<String>of(GANGLIA_MONITOR_ROLE, GANGLIA_METAD_ROLE))
        )
      ));
  }
  
}
