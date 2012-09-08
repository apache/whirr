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

package org.apache.whirr.service.pig;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

import java.io.IOException;

import static org.jclouds.scriptbuilder.domain.Statements.call;

/**
 * Pig cluster action handler which configures Pig by unpacking a binary tarball and
 * setting PIG_HOME and PATH environment variables.
 */
public class PigClientClusterActionHandler extends ClusterActionHandlerSupport {

  public final static String PIG_CLIENT_ROLE = "pig-client";

  final static String PIG_DEFAULT_PROPERTIES = "whirr-pig-default.properties";

  final static String PIG_TAR_URL = "whirr.pig.tarball.url";

  final static String PIG_CLIENT_SCRIPT = "configure_pig_client";

  final static String URL_FLAG = "-u";

  @Override
  public String getRole() {
    return PIG_CLIENT_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    Configuration conf = getConfiguration(event.getClusterSpec(), PIG_DEFAULT_PROPERTIES);

    String pigTarball = prepareRemoteFileUrl(event, conf.getString(PIG_TAR_URL));

    addStatement(event, call(PIG_CLIENT_SCRIPT, URL_FLAG, pigTarball));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
      handleFirewallRules(event);
  }
}
