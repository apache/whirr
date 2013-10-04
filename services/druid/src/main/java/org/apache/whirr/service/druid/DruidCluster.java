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

import org.apache.whirr.Cluster;
import org.apache.whirr.RolePredicates;

import java.io.IOException;

/**
 * DruidCluster - master class of the Druid cluster, with static methods.
 */
public class DruidCluster {
    /**
     * Returns the public address of the MySQL node.
     * @param cluster the Cluster object
     * @return mysql ip
     * @throws IOException
     */
    public static String getMySQLPublicAddress(Cluster cluster)
            throws IOException {
        return cluster.getInstanceMatching(
                RolePredicates.role(DruidMySQLClusterActionHandler.ROLE))
                .getPrivateIp();
    }

    /**
     * Converts a version to a url.
     * @param version - the druid release
     * @return version url
     */
    public static String getDownloadUrl(String version) {
        return "http://static.druid.io/artifacts/releases/druid-services-"
                + version + "-bin.tar.gz";
    }
}
