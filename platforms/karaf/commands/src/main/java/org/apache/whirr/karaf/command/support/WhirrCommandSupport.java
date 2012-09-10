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
import org.apache.whirr.InstanceTemplate;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.osgi.service.cm.ConfigurationAdmin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class WhirrCommandSupport extends OsgiCommandSupport {

  @Option(required = false, name = "--config", description = "The configuration file")
  protected String fileName;

  @Option(required = false, name = "--pid", description = "The PID of the configuration")
  protected String pid;

  @Option(required = false, name = "--provider", description = "The compute provider")
  protected String provider;

  @Option(required = false, name = "--endpoint", description = "The compute endpoint")
  protected String endpoint;

  @Option(required = false, name = "--templates", description = "The templates to use")
  protected String templates;

  @Option(required = false, name = "--imageId", description = "The image id")
  protected String imageId;

  @Option(required = false, name = "--locationId", description = "The location")
  protected String locationId;

  @Option(required = false, name = "--hardwareId", description = "The hardware")
  protected String hardwareId;

  @Option(required = false, name = "--cluster-name", description = "The name of the cluster")
  protected String clusterName;

  @Option(required = false, name = "--private-key-file", description = "The private ssh key")
  protected String privateKey;

  protected ClusterControllerFactory clusterControllerFactory;
  protected ConfigurationAdmin configurationAdmin;
  protected List<ComputeService> computeServices;


  public void validateInput() throws Exception {
    if (pid != null || fileName != null) {
       return;
    } else {
      if (provider == null || getComputeService(provider) == null) {
        throw new Exception("A proper configuration or a valid provider should be provided.");
      }
      if (templates == null) {
        throw new Exception("A proper configuration or a valid template should be specified");
      }
    }
  }

  public ComputeService getComputeService(String provider) {
    if (computeServices != null && !computeServices.isEmpty()) {
      for (ComputeService computeService : computeServices) {
        if (computeService.getContext().unwrap().getId().equals(provider)) {
          return computeService;
        }
      }
    }
    return null;
  }

  /**
   * Returns the {@link ClusterSpec}
   *
   * @return
   * @throws Exception
   */
  protected ClusterSpec getClusterSpec() throws Exception {
    ClusterSpec clusterSpec = null;
    if (pid != null || fileName != null) {
      PropertiesConfiguration properties = getConfiguration(pid, fileName);
      clusterSpec = new ClusterSpec(properties);
    } else {
      clusterSpec = new ClusterSpec();
    }



    if (provider != null) {
      clusterSpec.setProvider(provider);
    }

    if (endpoint != null) {
      clusterSpec.setEndpoint(endpoint);
    }

    if (templates != null) {
      clusterSpec.setInstanceTemplates(getTemplate(templates, imageId, hardwareId));
    }


    if (privateKey != null) {
      clusterSpec.setPrivateKey(privateKey);
    }
    if (clusterName != null) {
      clusterSpec.setClusterName(clusterName);
    }

    return clusterSpec;
  }

  protected InstanceTemplate.Builder getTemplateBuilder(String imageId, String hardwareId) {
    InstanceTemplate.Builder instanceBuilder = InstanceTemplate.builder();
    StringBuilder sb = new StringBuilder();
    String separator = "";
    if (imageId != null) {
      sb.append(separator).append("imageId=").append(imageId);
      separator = ",";
    }

    if (hardwareId != null) {
      sb.append(separator).append("hardwareId=").append(hardwareId);
    }
    instanceBuilder = instanceBuilder.template(TemplateBuilderSpec.parse(sb.toString()));
    return instanceBuilder;
  }

  protected List<InstanceTemplate> getTemplate(String templates, String imageId, String hardwareId) {
    List<InstanceTemplate> templateList = new ArrayList<InstanceTemplate>();
    Map<String, String> roleMap = InstanceTemplate.parse(templates.replaceAll("\\["," ").replaceAll("\\]"," ").split(","));
    for (Map.Entry<String, String> entry : roleMap.entrySet()) {
      InstanceTemplate.Builder builder = getTemplateBuilder(imageId, hardwareId);
      Integer numberOfInstances = Integer.parseInt(entry.getValue());
      String roles = entry.getKey();
      templateList.add(builder.numberOfInstance(numberOfInstances).roles(roles.split("\\+")).build());
    }
    return templateList;
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

  public void setComputeServices(List<ComputeService> computeServices) {
    this.computeServices = computeServices;
  }
}
