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
package org.apache.whirr.service.hama;

import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

/**
 * Base class for Hama service handlers.
 */
public abstract class HamaClusterActionHandler extends
    ClusterActionHandlerSupport {

  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with a hama defaults properties.
   */
  protected synchronized Configuration getConfiguration(ClusterSpec clusterSpec)
      throws IOException {
    return getConfiguration(clusterSpec, HamaConstants.HAMA_DEFAULT_PROPERTIES);
  }
  
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();

    addStatement(event, call("configure_hostnames"));
    addStatement(event, call("install_java"));
    addStatement(event, call("install_tarball"));

    String hamaInstallFunction = getConfiguration(clusterSpec).getString(
        HamaConstants.KEY_INSTALL_FUNCTION, HamaConstants.FUNCTION_INSTALL);

    String tarurl = prepareRemoteFileUrl(event, getConfiguration(clusterSpec)
        .getString(HamaConstants.KEY_TARBALL_URL));

    addStatement(event,
        call(hamaInstallFunction, HamaConstants.PARAM_TARBALL_URL, tarurl)
    );
  }
}
