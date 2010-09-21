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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.jclouds.http.Payloads.newStringPayload;
import static org.jclouds.http.Payloads.newFilePayload;
import static org.jclouds.util.Utils.toStringAndClose;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.jclouds.http.Payload;

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
    PUBLIC_KEY_FILE(String.class, false),
    PRIVATE_KEY_FILE(String.class, false),
    CLIENT_CIDRS(String.class, true),
    RUN_URL_BASE(String.class, false);
    
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

  private static final String DEFAULT_PROPERTIES = "whirr-default.properties";
  
  private List<InstanceTemplate> instanceTemplates;
  private String serviceName;
  private String provider;
  private String identity;
  private String credential;
  private String clusterName;
  private Payload privateKey;
  private Payload publicKey;
  private List<String> clientCidrs;
  private String runUrlBase;
  
  private Configuration config;
  
  public ClusterSpec() throws ConfigurationException {
    this(new PropertiesConfiguration());
  }
  
  /**
   * 
   * @throws ConfigurationException if the public or private key cannot be read.
   */
  public ClusterSpec(Configuration config)
      throws ConfigurationException {
    CompositeConfiguration c = new CompositeConfiguration();
    c.addConfiguration(config);
    c.addConfiguration(new PropertiesConfiguration(DEFAULT_PROPERTIES));
    setServiceName(c.getString(Property.SERVICE_NAME.getConfigName()));
    setInstanceTemplates(InstanceTemplate.parse(
        c.getStringArray(Property.INSTANCE_TEMPLATES.getConfigName())));
    setProvider(c.getString(Property.PROVIDER.getConfigName()));
    setIdentity(c.getString(Property.IDENTITY.getConfigName()));
    setCredential(c.getString(Property.CREDENTIAL.getConfigName()));
    setClusterName(c.getString(Property.CLUSTER_NAME.getConfigName()));
    try {
      String privateKeyPath = c.getString(
          Property.PRIVATE_KEY_FILE.getConfigName());
      if (privateKeyPath != null)
        setPrivateKey(new File(privateKeyPath));
      String publicKeyPath = c.getString(Property.PUBLIC_KEY_FILE.
               getConfigName());
      publicKeyPath = publicKeyPath == null && privateKeyPath != null ?
                privateKeyPath + ".pub" : publicKeyPath;
      if (publicKeyPath != null)
        setPublicKey(new File(publicKeyPath));
    } catch (IOException e) {
      throw new ConfigurationException("error reading key from file", e);
    }
    setClientCidrs(c.getList(Property.CLIENT_CIDRS.getConfigName()));
    setRunUrlBase(c.getString(Property.RUN_URL_BASE.getConfigName()));
    this.config = c;
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
  public Payload getPrivateKey() {
    return privateKey;
  }
  public Payload getPublicKey() {
    return publicKey;
  }
  /**
   * @see #getPrivateKey
   * @throws IOException if the payload cannot be read
   */
  public String readPrivateKey() throws IOException {
    return toStringAndClose(getPrivateKey().getContent());
  }
  /**
   * @see #getPublicKey
   * @throws IOException if the payload cannot be read
   */
  public String readPublicKey() throws IOException {
    return toStringAndClose(getPublicKey().getContent());
  }
  public List<String> getClientCidrs() {
    return clientCidrs;
  }
  
  public String getRunUrlBase() {
    return runUrlBase;
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

  /**
   * The rsa public key which is authorized to login to your on the cloud nodes.
   * 
   * @param publicKey
   */
  public void setPublicKey(String publicKey) {
    checkArgument(checkNotNull(publicKey, "publicKey").startsWith("ssh-rsa"),
        "key should start with ssh-rsa");
    this.publicKey = newStringPayload(publicKey);
  }
  
  /**
   * 
   * @throws IOException
   *           if there is a problem reading the file
   * @see #setPublicKey(String)
   */
  public void setPublicKey(File publicKey) throws IOException {
    this.publicKey = newFilePayload(checkNotNull(publicKey,
        "publicKey"));
  }

  /**
   * The rsa private key which is used as the login identity on the cloud 
   * nodes.
   * 
   * @param privateKey
   */
  public void setPrivateKey(String privateKey) {
    checkArgument(checkNotNull(privateKey, "privateKey")
        .startsWith("-----BEGIN RSA PRIVATE KEY-----"),
        "key should start with -----BEGIN RSA PRIVATE KEY-----");
    this.privateKey = newStringPayload(privateKey);
  }

  /**
   * 
   * @throws IOException
   *           if there is a problem reading the file
   * @see #setPrivateKey(String)
   */
  public void setPrivateKey(File privateKey) throws IOException {
    this.privateKey = newFilePayload(
        checkNotNull(privateKey, "privateKey"));
  }

  public void setClientCidrs(List<String> clientCidrs) {
    this.clientCidrs = clientCidrs;
  }

  public void setRunUrlBase(String runUrlBase) {
    this.runUrlBase = runUrlBase;
  }
  
  //
  
  public Configuration getConfiguration() {
    return config;
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
        && Objects.equal(clientCidrs, that.clientCidrs)
        && Objects.equal(runUrlBase, that.runUrlBase)
        ;
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hashCode(instanceTemplates, serviceName,
        provider, identity, credential, clusterName, publicKey,
        privateKey, clientCidrs, runUrlBase);
  }
  
  public String toString() {
    return Objects.toStringHelper(this)
      .add("instanceTemplates", instanceTemplates)
      .add("serviceName", serviceName)
      .add("provider", provider)
      .add("identity", identity)
      .add("credential", credential)
      .add("clusterName", clusterName)
      .add("publicKey", publicKey)
      .add("privateKey", privateKey)
      .add("clientCidrs", clientCidrs)
      .add("runUrlBase", runUrlBase)
      .toString();
  }
  
}
