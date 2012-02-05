/*
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

package org.apache.whirr.karaf.command.support;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.felix.gogo.commands.Option;
import org.apache.karaf.shell.console.OsgiCommandSupport;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.ComputeService;
import org.osgi.service.cm.ConfigurationAdmin;

import java.util.List;

public abstract class WhirrCommandSupport extends OsgiCommandSupport {

  @Option(required = false, name = "--config", description = "The configuration file")
  protected String fileName;

  @Option(required = false, name = "--pid", description = "The PID of the configuration")
  protected String pid;

  @Option(required = false, name = "--cluster-name", description = "The name of the cluster")
  protected String clusterName;

  @Option(required = false, name = "--private-key-file", description = "The private ssh key")
  protected String privateKey;

  protected ClusterControllerFactory clusterControllerFactory;
  protected ConfigurationAdmin configurationAdmin;
  protected List<ComputeService> computeServices;

  /**
   * Returns the {@link ClusterSpec}
   *
   * @return
   * @throws Exception
   */
  protected ClusterSpec getClusterSpec() throws Exception {
    ClusterSpec clusterSpec = null;
    PropertiesConfiguration properties = getConfiguration(pid, fileName);
    if (properties != null) {
      clusterSpec = new ClusterSpec(properties);
      if (privateKey != null) {
        clusterSpec.setPrivateKey(privateKey);
      }
      if (clusterName != null) {
        clusterSpec.setClusterName(clusterName);
      }
    }
    return clusterSpec;
  }

  /**
   * Retrieves the configuration from a pid or a file.
   *
   * @param pid
   * @param fileName
   * @return
   */
  protected PropertiesConfiguration getConfiguration(String pid, String fileName) {
    if (pid != null) {
      return ConfigurationReader.fromConfigAdmin(configurationAdmin, pid);
    } else if (fileName != null) {
      return ConfigurationReader.fromFile(fileName);
    } else {
      System.err.println("A valid pid or a file must be specified for configuration");
    }
    return null;
  }

  public ClusterControllerFactory getClusterControllerFactory() {
    return clusterControllerFactory;
  }

  public void setClusterControllerFactory(ClusterControllerFactory clusterControllerFactory) {
    this.clusterControllerFactory = clusterControllerFactory;
  }

  public ConfigurationAdmin getConfigurationAdmin() {
    return configurationAdmin;
  }

  public void setConfigurationAdmin(ConfigurationAdmin configurationAdmin) {
    this.configurationAdmin = configurationAdmin;
  }

  public List<ComputeService> getComputeServices() {
    return computeServices;
  }

  public void setComputeServices(List<ComputeService> computeServices) {
    this.computeServices = computeServices;
  }
}
