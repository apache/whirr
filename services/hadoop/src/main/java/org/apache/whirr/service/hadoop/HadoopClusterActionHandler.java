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

package org.apache.whirr.service.hadoop;

import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.ClusterSpec;

public abstract class HadoopClusterActionHandler extends ClusterActionHandlerSupport {

  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with a hadoop defaults
   * properties.
   *
   * @param clusterSpec  The cluster specification instance.
   * @return The composite configuration.
   */
  protected synchronized Configuration getConfiguration(
      ClusterSpec clusterSpec) throws IOException {
    try {
      Configuration defaults = new PropertiesConfiguration(
        "whirr-hadoop-default.properties");
      return super.getConfiguration(clusterSpec, defaults);
    } catch (ConfigurationException e) {
      throw new IOException("Error loading Hadoop default properties.", e);
    }
  }
  
  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    ClusterSpec clusterSpec = event.getClusterSpec();
    Configuration conf = getConfiguration(clusterSpec);
    addStatement(event, call("configure_hostnames", "-c", clusterSpec.getProvider()));
    String hadoopInstallFunction = conf.getString(
        "whirr.hadoop-install-function", "install_hadoop");
    addStatement(event, call("install_java"));
    addStatement(event, call("install_tarball"));
    String tarball = conf.getString("whirr.hadoop.tarball.url");
    addStatement(event, call(hadoopInstallFunction, "-c", clusterSpec.getProvider(),
        "-u", tarball));
  }
}
