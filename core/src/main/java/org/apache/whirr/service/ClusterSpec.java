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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.interpol.ConfigurationInterpolator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.whirr.ssh.KeyPair.sameKeyPair;


/**
 * This class represents the specification of a cluster. It is used to describe
 * the properties of a cluster before it is launched.
 */
public class ClusterSpec {
  
  static {
    // Environment variable interpolation (e.g. {env:MY_VAR}) is supported
    // natively in Commons Configuration 1.7, but until it's released we
    // do it ourselves here
    ConfigurationInterpolator.registerGlobalLookup("env", new StrLookup() {
      @Override
      public String lookup(String key) {
        return System.getenv(key);
      }
    });
  }
  
  public enum Property {
    SERVICE_NAME(String.class, false, "(optional) The name of the " + 
      "service to use. E.g. hadoop."),
      
    INSTANCE_TEMPLATES(String.class, false, "The number of instances " +
      "to launch for each set of roles. E.g. 1 hadoop-namenode+" +
      "hadoop-jobtracker, 10 hadoop-datanode+hadoop-tasktracker"),
      
    PROVIDER(String.class, false, "The name of the cloud provider. " + 
      "E.g. aws-ec2, cloudservers-uk"),
      
    CREDENTIAL(String.class, false, "The cloud credential."),
    
    IDENTITY(String.class, false, "The cloud identity."),
    
    CLUSTER_NAME(String.class, false,  "The name of the cluster " + 
      "to operate on. E.g. hadoopcluster."),
      
    PUBLIC_KEY_FILE(String.class, false, "The filename of the public " +
      "key used to connect to instances."),
      
    PRIVATE_KEY_FILE(String.class, false, "The filename of the " + 
      "private RSA key used to connect to instances."),
      
    IMAGE_ID(String.class, false, "The ID of the image to use for " + 
      "instances. If not specified then a vanilla Linux image is " + 
      "chosen."),
      
    HARDWARE_ID(String.class, false, "The type of hardware to use for" + 
      " the instance. This must be compatible with the image ID."),
      
    LOCATION_ID(String.class, false, "The location to launch " + 
      "instances in. If not specified then an arbitrary location " + 
      "will be chosen."),
      
    CLIENT_CIDRS(String.class, true, "A comma-separated list of CIDR" + 
      " blocks. E.g. 208.128.0.0/11,108.128.0.0/11"),
      
    VERSION(String.class, false, ""),
    
    RUN_URL_BASE(String.class, false, "The base URL for forming run " + 
      "urls from. Change this to host your own set of launch scripts.");
    
    private Class<?> type;
    private boolean multipleArguments;
    private String description;
    
    Property(Class<?> type, boolean multipleArguments, String description) {
      this.type = type;
      this.multipleArguments = multipleArguments;
      this.description = description;
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
    
    public String getDescription() {
      return description;
    }
  }
  
  /**
   * This class describes the type of instances that should be in the cluster.
   * This is done by specifying the number of instances in each role.
   */
  public static class InstanceTemplate {
    private static Map<String, String> aliases = new HashMap<String, String>();
    private static final Logger LOG = LoggerFactory.getLogger(InstanceTemplate.class);

    static {
      /*
       * WARNING: this is not a generic aliasing mechanism. This code
       * should be removed in the following releases and it's
       * used only temporary to deprecate short legacy role names.
       */
      aliases.put("nn", "hadoop-namenode");
      aliases.put("jt", "hadoop-jobtracker");
      aliases.put("dn", "hadoop-datanode");
      aliases.put("tt", "hadoop-tasktracker");
      aliases.put("zk", "zookeeper");
    }

    private Set<String> roles;
    private int numberOfInstances;

    public InstanceTemplate(int numberOfInstances, String... roles) {
      this(numberOfInstances, Sets.newLinkedHashSet(Lists.newArrayList(roles)));
    }

    public InstanceTemplate(int numberOfInstances, Set<String> roles) {
      for (String role : roles) {
        checkArgument(!StringUtils.contains(role, " "),
            "Role '%s' may not contain space characters.", role);
      }

      this.roles = replaceAliases(roles);
      this.numberOfInstances = numberOfInstances;
    }

    private Set<String> replaceAliases(Set<String> roles) {
      Set<String> newRoles = Sets.newLinkedHashSet();
      for(String role : roles) {
        if (aliases.containsKey(role)) {
          LOG.warn("Role name '{}' is deprecated, use '{}'",
              role, aliases.get(role));
          newRoles.add(aliases.get(role));
        } else {
          newRoles.add(role);
        }
      }
      return newRoles;
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
        checkArgument(parts.length == 2, 
            "Invalid instance template syntax for '%s'. Does not match " +
            "'<number> <role1>+<role2>+<role3>...', e.g. '1 nn+jt'.", s);
        int num = Integer.parseInt(parts[0]);
        templates.add(new InstanceTemplate(num, parts[1].split("\\+")));
      }
      return templates;
    }
  }

  private static final String DEFAULT_PROPERTIES = "whirr-default.properties";

  /**
   * Create an instance that uses a temporary RSA key pair.
   */
  @VisibleForTesting
  public static ClusterSpec withTemporaryKeys()
  throws ConfigurationException, JSchException, IOException {
    return withTemporaryKeys(new PropertiesConfiguration());
  }
  @VisibleForTesting
  public static ClusterSpec withTemporaryKeys(Configuration conf) 
  throws ConfigurationException, JSchException, IOException {
    if (!conf.containsKey(Property.PRIVATE_KEY_FILE.getConfigName())) {
      Map<String, File> keys = org.apache.whirr.ssh.KeyPair.generateTemporaryFiles();

      LoggerFactory.getLogger(ClusterSpec.class).debug("ssh keys: " +
            keys.toString());
      
      conf.addProperty(Property.PRIVATE_KEY_FILE.getConfigName(), 
        keys.get("private").getAbsolutePath());
      conf.addProperty(Property.PUBLIC_KEY_FILE.getConfigName(), 
        keys.get("public").getAbsolutePath());
    }
    
    return new ClusterSpec(conf);
  }

  /**
   * Create new empty instance for testing.
   */
  @VisibleForTesting
  public static ClusterSpec withNoDefaults() throws ConfigurationException {
    return withNoDefaults(new PropertiesConfiguration());
  }
  @VisibleForTesting
  public static ClusterSpec withNoDefaults(Configuration conf)
  throws ConfigurationException {
    return new ClusterSpec(conf, false);
  }

  private List<InstanceTemplate> instanceTemplates;
  private String serviceName;
  private String provider;
  private String identity;
  private String credential;
  private String clusterName;
  private String privateKey;
  private File privateKeyFile;
  private String publicKey;
  private String imageId;
  private String hardwareId;
  private String locationId;
  private List<String> clientCidrs;
  private String version;
  private String runUrlBase;
  
  private Configuration config;
  
  public ClusterSpec() throws ConfigurationException {
    this(new PropertiesConfiguration());
  }

  public ClusterSpec(Configuration config) throws ConfigurationException {
      this(config, true); // load default configs
  }

  /**
   * 
   * @throws ConfigurationException if something is wrong
   */
  public ClusterSpec(Configuration config, boolean loadDefaults)
      throws ConfigurationException {

    CompositeConfiguration c = new CompositeConfiguration();
    c.addConfiguration(config);
    if (loadDefaults) {
      c.addConfiguration(new PropertiesConfiguration(DEFAULT_PROPERTIES));
    }

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

      String publicKeyPath = c.getString(Property.PUBLIC_KEY_FILE.getConfigName());
      publicKeyPath = (publicKeyPath == null && privateKeyPath != null) ?
                privateKeyPath + ".pub" : publicKeyPath;
      if(privateKeyPath != null && publicKeyPath != null) {
        KeyPair pair = KeyPair.load(new JSch(), privateKeyPath, publicKeyPath);
        if (pair.isEncrypted()) {
          throw new ConfigurationException("Key pair is encrypted");
        }
        if (!sameKeyPair(new File(privateKeyPath), new File(publicKeyPath))) {
          throw new ConfigurationException("Both keys should belong " +
              "to the same key pair");
        }

        setPrivateKey(new File(privateKeyPath));
        setPublicKey(new File(publicKeyPath));
      }
    } catch (JSchException e) {
      throw new ConfigurationException("Invalid key pair", e);

    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("Invalid key", e);

    } catch (IOException e) {
      throw new ConfigurationException("Error reading one of key file", e);
    }

    setImageId(config.getString(Property.IMAGE_ID.getConfigName()));
    setHardwareId(config.getString(Property.HARDWARE_ID.getConfigName()));
    setLocationId(config.getString(Property.LOCATION_ID.getConfigName()));
    setClientCidrs(c.getList(Property.CLIENT_CIDRS.getConfigName()));
    setVersion(c.getString(Property.VERSION.getConfigName()));
    String runUrlBase = c.getString(Property.RUN_URL_BASE.getConfigName());

    if (runUrlBase == null && getVersion() != null) {
      try {
        runUrlBase = String.format("http://whirr.s3.amazonaws.com/%s/",
            URLEncoder.encode(getVersion(), "UTF-8"));
      } catch (UnsupportedEncodingException e) {
        throw new ConfigurationException(e);
      }
    }
    setRunUrlBase(runUrlBase);

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
    return getInstanceTemplate(Sets.newLinkedHashSet(Lists.newArrayList(roles)));
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
  public String getPrivateKey() {
    return privateKey;
  }
  public File getPrivateKeyFile() {
     return privateKeyFile;
   }
  public String getPublicKey() {
    return publicKey;
  }
  public String getImageId() {
    return imageId;
  }
  public String getHardwareId() {
    return hardwareId;
  }
  public String getLocationId() {
    return locationId;
  }
  public List<String> getClientCidrs() {
    return clientCidrs;
  }
  public String getVersion() {
    return version;
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
    checkPublicKey(publicKey);
    this.publicKey = publicKey;
  }
  
  /**
   * 
   * @throws IOException
   *           if there is a problem reading the file
   * @see #setPublicKey(String)
   */
  public void setPublicKey(File publicKey) throws IOException {
    String key = IOUtils.toString(new FileReader(publicKey));
    checkPublicKey(key);
    this.publicKey = key;
  }

  private void checkPublicKey(String publicKey) {
    /*
     * http://stackoverflow.com/questions/2494645#2494645
     */
    checkArgument(checkNotNull(publicKey, "publicKey")
            .startsWith("ssh-rsa AAAAB3NzaC1yc2EA"),
        "key should start with ssh-rsa AAAAB3NzaC1yc2EA");
  }
  
  /**
   * The rsa private key which is used as the login identity on the cloud 
   * nodes.
   * 
   * @param privateKey
   */
  public void setPrivateKey(String privateKey) {
    checkPrivateKey(privateKey);
    this.privateKey = privateKey;
  }

  /**
   * 
   * @throws IOException
   *           if there is a problem reading the file
   * @see #setPrivateKey(String)
   */
  public void setPrivateKey(File privateKey) throws IOException {
    this.privateKeyFile = privateKey;
    String key = IOUtils.toString(new FileReader(privateKey));
    checkPrivateKey(key);
    this.privateKey = key;
  }
  
  private void checkPrivateKey(String privateKey) {
    checkArgument(checkNotNull(privateKey, "privateKey")
        .startsWith("-----BEGIN RSA PRIVATE KEY-----"),
        "key should start with -----BEGIN RSA PRIVATE KEY-----");
  }

  public void setImageId(String imageId) {
    this.imageId = imageId;
  }
  
  public void setHardwareId(String hardwareId) {
    this.hardwareId = hardwareId;
  }
  
  public void setLocationId(String locationId) {
    this.locationId = locationId;
  }
  
  public void setClientCidrs(List<String> clientCidrs) {
    this.clientCidrs = clientCidrs;
  }
  
  public void setVersion(String version) {
    this.version = version;
  }

  public void setRunUrlBase(String runUrlBase) {
    this.runUrlBase = runUrlBase;
  }
  
  //
  
  public Configuration getConfiguration() {
    return config;
  }
  
  public Configuration getConfigurationForKeysWithPrefix(String prefix) {
    Configuration c = new PropertiesConfiguration();
    for (@SuppressWarnings("unchecked")
        Iterator<String> it = config.getKeys(prefix); it.hasNext(); ) {
      String key = it.next();
      c.setProperty(key, config.getProperty(key));
    }
    return c;
  }
  
  /**
   * @return the directory for storing cluster-related files
   */
  public File getClusterDirectory() {
    File clusterDir = new File(new File(System.getProperty("user.home")),
        ".whirr");
    clusterDir = new File(clusterDir, getClusterName());
    clusterDir.mkdirs();
    return clusterDir;
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
        && Objects.equal(imageId, that.imageId)
        && Objects.equal(hardwareId, that.hardwareId)
        && Objects.equal(locationId, that.locationId)
        && Objects.equal(clientCidrs, that.clientCidrs)
        && Objects.equal(version, that.version)
        && Objects.equal(runUrlBase, that.runUrlBase)
        ;
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hashCode(instanceTemplates, serviceName,
        provider, identity, credential, clusterName, publicKey,
        privateKey, imageId, hardwareId, locationId, clientCidrs, version,
        runUrlBase);
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
      .add("imageId", imageId)
      .add("instanceSizeId", hardwareId)
      .add("locationId", locationId)
      .add("clientCidrs", clientCidrs)
      .add("version", version)
      .add("runUrlBase", runUrlBase)
      .toString();
  }
  
}
