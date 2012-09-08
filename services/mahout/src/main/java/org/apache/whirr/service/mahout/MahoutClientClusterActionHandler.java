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

package org.apache.whirr.service.mahout;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

import java.io.IOException;

import static org.jclouds.scriptbuilder.domain.Statements.call;

/**
 * Mahout cluster action handler which configures Mahout by unpacking a binary tarball and
 * setting MAHOUT_HOME and PATH environment variables.
 */
public class MahoutClientClusterActionHandler extends ClusterActionHandlerSupport {

  public final static String MAHOUT_CLIENT_ROLE = "mahout-client";

  final static String MAHOUT_DEFAULT_PROPERTIES = "whirr-mahout-default.properties";

  final static String MAHOUT_TAR_URL = "whirr.mahout.tarball.url";

  final static String MAHOUT_CLIENT_SCRIPT = "configure_mahout_client";

  final static String URL_FLAG = "-u";

  @Override
  public String getRole() {
    return MAHOUT_CLIENT_ROLE;
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException, InterruptedException {
    Configuration conf = getConfiguration(event.getClusterSpec(), MAHOUT_DEFAULT_PROPERTIES);

    String mahoutTarball = prepareRemoteFileUrl(event, conf.getString(MAHOUT_TAR_URL));

    addStatement(event, call("retry_helpers"));
    addStatement(event, call(MAHOUT_CLIENT_SCRIPT, URL_FLAG, mahoutTarball));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException, InterruptedException {
      handleFirewallRules(event);
  }
}
