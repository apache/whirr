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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;

/**
 * This class represents the specification of a cluster. It is used to describe
 * the properties of a cluster before it is launched.
 */
public class ClusterSpec {
  
  public enum Property {
    SERVICE_NAME(String.class, false),
    INSTANCE_TEMPLATES(String.class, false),
    PROVIDER(String.class, false),
    CREDENTIAL(String.class, false),
    IDENTITY(String.class, false),
    CLUSTER_NAME(String.class, false),
    SECRET_KEY_FILE(String.class, false),
    CLIENT_CIDRS(String.class, true);
    
    private Class<?> type;
    private boolean multipleArguments;
    Property(Class<?> type, boolean multipleArguments) {
      this.type = type;
      this.multipleArguments = multipleArguments;
    }
    
    public String getSimpleName() {
      return name().toLowerCase().replace('_', '-');
    }

    public String getConfigName() {
      return "whirr." + getSimpleName();
    }
    
    public Class<?> getType() {
      return type;
    }
    
    public boolean hasMultipleArguments() {
      return multipleArguments;
    }
  }
  
  /**
   * This class describes the type of instances that should be in the cluster.
   * This is done by specifying the number of instances in each role.
   */
  public static class InstanceTemplate {
    private Set<String> roles;
    private int numberOfInstances;

    public InstanceTemplate(int numberOfInstances, String... roles) {
      this(numberOfInstances, Sets.newHashSet(roles));
    }

    public InstanceTemplate(int numberOfInstances, Set<String> roles) {
      this.numberOfInstances = numberOfInstances;
      this.roles = roles;
    }

    public Set<String> getRoles() {
      return roles;
    }

    public int getNumberOfInstances() {
      return numberOfInstances;
    }
    
    public boolean equals(Object o) {
      if (o instanceof InstanceTemplate) {
        InstanceTemplate that = (InstanceTemplate) o;
        return Objects.equal(numberOfInstances, that.numberOfInstances)
          && Objects.equal(roles, that.roles);
      }
      return false;
    }
    
    public int hashCode() {
      return Objects.hashCode(numberOfInstances, roles);
    }
    
    public String toString() {
      return Objects.toStringHelper(this)
        .add("numberOfInstances", numberOfInstances)
        .add("roles", roles)
        .toString();
    }
    
    public static List<InstanceTemplate> parse(String... strings) {
      List<InstanceTemplate> templates = Lists.newArrayList();
      for (String s : strings) {
        String[] parts = s.split(" ");
        int num = Integer.parseInt(parts[0]);
        templates.add(new InstanceTemplate(num, parts[1].split("\\+")));
      }
      return templates;
    }
  }
  
  private List<InstanceTemplate> instanceTemplates = Lists.newArrayList();
  private String serviceName;
  private String provider;
  private String identity;
  private String credential;
  private String clusterName;
  private String secretKeyFile;
  private List<String> clientCidrs = Lists.newArrayList();
  
  public static ClusterSpec fromConfiguration(Configuration config) {
    ClusterSpec spec = new ClusterSpec();
    spec.setServiceName(config.getString(Property.SERVICE_NAME.getConfigName()));
    spec.setInstanceTemplates(InstanceTemplate.parse(
        config.getStringArray(Property.INSTANCE_TEMPLATES.getConfigName())));
    spec.setProvider(config.getString(Property.PROVIDER.getConfigName()));
    spec.setIdentity(checkNotNull(
        config.getString(Property.IDENTITY.getConfigName()), Property.IDENTITY));
    spec.setCredential(config.getString(Property.CREDENTIAL.getConfigName()));
    spec.setClusterName(config.getString(Property.CLUSTER_NAME.getConfigName()));
    spec.setSecretKeyFile(config.getString(Property.SECRET_KEY_FILE.getConfigName()));
    spec.setClientCidrs(config.getList(Property.CLIENT_CIDRS.getConfigName()));
    return spec;
  }

  public List<InstanceTemplate> getInstanceTemplates() {
    return instanceTemplates;
  }
  
  public InstanceTemplate getInstanceTemplate(final Set<String> roles) {
    for (InstanceTemplate template : instanceTemplates) {
      if (roles.equals(template.roles)) {
        return template;
      }
    }
    return null;
  }
  
  public InstanceTemplate getInstanceTemplate(String... roles) {
    return getInstanceTemplate(Sets.newHashSet(roles));
  }
  
  public String getServiceName() {
    return serviceName;
  }
  public String getProvider() {
    return provider;
  }
  public String getIdentity() {
    return identity;
  }
  public String getCredential() {
    return credential;
  }
  public String getClusterName() {
    return clusterName;
  }
  public String getSecretKeyFile() {
    return secretKeyFile;
  }
  public List<String> getClientCidrs() {
    return clientCidrs;
  }
  
  public void setInstanceTemplates(List<InstanceTemplate> instanceTemplates) {
    this.instanceTemplates = instanceTemplates;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public void setProvider(String provider) {
    this.provider = provider;
  }

  public void setIdentity(String identity) {
    this.identity = identity;
  }

  public void setCredential(String credential) {
    this.credential = credential;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setSecretKeyFile(String secretKeyFile) {
    this.secretKeyFile = secretKeyFile;
  }

  public void setClientCidrs(List<String> clientCidrs) {
    this.clientCidrs = clientCidrs;
  }

  //
  public String readPrivateKey() throws IOException {
    return Files.toString(new File(getSecretKeyFile()), Charsets.UTF_8);
  }
    
  public String readPublicKey() throws IOException {
    return Files.toString(new File(getSecretKeyFile() + ".pub"), Charsets.UTF_8);
  }
  
  public boolean equals(Object o) {
    if (o instanceof ClusterSpec) {
      ClusterSpec that = (ClusterSpec) o;
      return Objects.equal(instanceTemplates, that.instanceTemplates)
        && Objects.equal(serviceName, that.serviceName)
        && Objects.equal(provider, that.provider)
        && Objects.equal(identity, that.identity)
        && Objects.equal(credential, that.credential)
        && Objects.equal(clusterName, that.clusterName)
        && Objects.equal(secretKeyFile, that.secretKeyFile)
        && Objects.equal(clientCidrs, that.clientCidrs);
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hashCode(instanceTemplates, serviceName,
        provider, identity, credential, clusterName, secretKeyFile,
        clientCidrs);
  }
  
  public String toString() {
    return Objects.toStringHelper(this)
      .add("instanceTemplates", instanceTemplates)
      .add("serviceName", serviceName)
      .add("provider", provider)
      .add("identity", identity)
      .add("credential", credential)
      .add("clusterName", clusterName)
      .add("secretKeyFile", secretKeyFile)
      .add("clientCidrs", clientCidrs)
      .toString();
  }
  
}
