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

package org.apache.whirr;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.whirr.util.KeyPair.sameKeyPair;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.interpol.ConfigurationInterpolator;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.text.StrLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class represents the specification of a cluster. It is used to describe
 * the properties of a cluster before it is launched.
 */
public class ClusterSpec {
  
  private static final Logger LOG = LoggerFactory.getLogger(ClusterSpec.class);
  
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
    CLUSTER_NAME(String.class, false,  "The name of the cluster " +
      "to operate on. E.g. hadoopcluster."),

    SERVICE_NAME(String.class, false, "(optional) The name of the " +
      "service to use. E.g. hadoop."),

    LOGIN_USER(String.class, false,  "Override the default login user "+
      "used to bootstrap whirr. E.g. ubuntu or myuser:mypass."),

    CLUSTER_USER(String.class, false, "The name of the user that Whirr " +
            "will create on all the cluster instances. You have to use " +
            "this user to login to nodes."),

    INSTANCE_TEMPLATES(String.class, false, "The number of instances " +
      "to launch for each set of roles. E.g. 1 hadoop-namenode+" +
      "hadoop-jobtracker, 10 hadoop-datanode+hadoop-tasktracker"),
      
    INSTANCE_TEMPLATES_MAX_PERCENT_FAILURES(String.class, false, "The percentage " +
      "of successfully started instances for each set of roles. E.g. " + 
      "100 hadoop-namenode+hadoop-jobtracker,60 hadoop-datanode+hadoop-tasktracker means " + 
      "all instances with the roles hadoop-namenode and hadoop-jobtracker " + 
      "has to be successfully started, and 60% of instances has to be succcessfully " + 
      "started each with the roles hadoop-datanode and hadoop-tasktracker."),

    INSTANCE_TEMPLATES_MINIMUM_NUMBER_OF_INSTANCES(String.class, false, "The minimum number" +
      "of successfully started instances for each set of roles. E.g. " +
      "1 hadoop-namenode+hadoop-jobtracker,6 hadoop-datanode+hadoop-tasktracker means " + 
      "1 instance with the roles hadoop-namenode and hadoop-jobtracker has to be successfully started," +
      " and 6 instances has to be successfully started each with the roles hadoop-datanode and hadoop-tasktracker."),

    MAX_STARTUP_RETRIES(Integer.class, false, "The number of retries in case of insufficient " + 
        "successfully started instances. Default value is 1."),
    
    PROVIDER(String.class, false, "The name of the cloud provider. " + 
      "E.g. aws-ec2, cloudservers-uk"),
      
    CREDENTIAL(String.class, false, "The cloud credential."),
    
    IDENTITY(String.class, false, "The cloud identity."),

    PUBLIC_KEY_FILE(String.class, false, "The filename of the public " +
      "key used to connect to instances."),
      
    PRIVATE_KEY_FILE(String.class, false, "The filename of the " + 
      "private RSA key used to connect to instances."),

    BLOBSTORE_PROVIDER(String.class, false, "The blob store provider. " +
      "E.g. aws-s3, cloudfiles-us, cloudfiles-uk"),

    BLOBSTORE_IDENTITY(String.class, false, "The blob store identity"),

    BLOBSTORE_CREDENTIAL(String.class, false, "The blob store credential"),

    BLOBSTORE_LOCATION_ID(String.class, false, "The blob store location ID"),
      
    IMAGE_ID(String.class, false, "The ID of the image to use for " + 
      "instances. If not specified then a vanilla Linux image is " + 
      "chosen."),
      
    HARDWARE_ID(String.class, false, "The type of hardware to use for" + 
      " the instance. This must be compatible with the image ID."),

    HARDWARE_MIN_RAM(Integer.class, false, "The minimum amount of " +
      "instance memory. E.g. 1024"),
      
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
      Map<String, File> keys = org.apache.whirr.util.KeyPair.generateTemporaryFiles();

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

  private String clusterName;
  private String serviceName;

  private String clusterUser;
  private String loginUser;

  private List<InstanceTemplate> instanceTemplates;
  private int maxStartupRetries;

  private String provider;
  private String identity;
  private String credential;

  private String blobStoreProvider;
  private String blobStoreIdentity;
  private String blobStoreCredential;

  private String privateKey;
  private File privateKeyFile;
  private String publicKey;

  private String locationId;
  private String blobStoreLocationId;

  private String imageId;

  private String hardwareId;
  private int hardwareMinRam;

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
   * @throws ConfigurationException if something is wrong
   */
  public ClusterSpec(Configuration userConfig, boolean loadDefaults)
      throws ConfigurationException {

    if (loadDefaults) {
      config = composeWithDefaults(userConfig);
    } else {
      config = ConfigurationUtils.cloneConfiguration(userConfig);
    }

    setClusterName(getString(Property.CLUSTER_NAME));
    setServiceName(getString(Property.SERVICE_NAME));

    setLoginUser(getString(Property.LOGIN_USER));
    setClusterUser(getString(Property.CLUSTER_USER));

    setInstanceTemplates(InstanceTemplate.parse(config));
    setMaxStartupRetries(getInt(Property.MAX_STARTUP_RETRIES, 1));

    setProvider(getString(Property.PROVIDER));
    setIdentity(getString(Property.IDENTITY));
    setCredential(getString(Property.CREDENTIAL));

    setBlobStoreProvider(getString(Property.BLOBSTORE_PROVIDER));
    setBlobStoreIdentity(getString(Property.BLOBSTORE_IDENTITY));
    setBlobStoreCredential(getString(Property.BLOBSTORE_CREDENTIAL));

    checkAndSetKeyPair();

    setImageId(getString(Property.IMAGE_ID));
    setHardwareId(getString(Property.HARDWARE_ID));
    setHardwareMinRam(getInt(Property.HARDWARE_MIN_RAM, 1024));

    setLocationId(getString(Property.LOCATION_ID));
    setBlobStoreLocationId(getString(Property.BLOBSTORE_LOCATION_ID));
    setClientCidrs(getList(Property.CLIENT_CIDRS));

    setVersion(getString(Property.VERSION));
    setRunUrlBase(getString(Property.RUN_URL_BASE));
  }

  private String getString(Property key) {
    return config.getString(key.getConfigName());
  }

  private int getInt(Property key, int defaultValue) {
    return config.getInt(key.getConfigName(), defaultValue);
  }

  private List<String> getList(Property key) {
    return config.getList(key.getConfigName());
  }

  private Configuration composeWithDefaults(Configuration userConfig)
      throws ConfigurationException {
    CompositeConfiguration composed = new CompositeConfiguration();
    composed.addConfiguration(userConfig);
    composed.addConfiguration(
      new PropertiesConfiguration(DEFAULT_PROPERTIES));
    return composed;
  }

  private void checkAndSetKeyPair() throws ConfigurationException {
    String pairRepresentation = "";
    try {
      String privateKeyPath = getString(Property.PRIVATE_KEY_FILE);

      String publicKeyPath = getString(Property.PUBLIC_KEY_FILE);
      publicKeyPath = (publicKeyPath == null && privateKeyPath != null) ?
                privateKeyPath + ".pub" : publicKeyPath;
      if(privateKeyPath != null && publicKeyPath != null) {
        pairRepresentation = "(" + privateKeyPath + ", " +
            publicKeyPath + ")";
        KeyPair pair = KeyPair.load(new JSch(), privateKeyPath, publicKeyPath);
        if (pair.isEncrypted()) {
          throw new ConfigurationException("Key pair " + pairRepresentation +
              " is encrypted. Try generating a new passwordless SSH keypair " +
              "(e.g. with ssh-keygen).");
        }
        if (!sameKeyPair(new File(privateKeyPath), new File(publicKeyPath))) {
          throw new ConfigurationException("Both keys should belong " +
              "to the same key pair: " + pairRepresentation);
        }

        setPrivateKey(new File(privateKeyPath));
        setPublicKey(new File(publicKeyPath));
      }
    } catch (JSchException e) {
      throw new ConfigurationException("Invalid key pair: " +
          pairRepresentation, e);

    } catch (IllegalArgumentException e) {
      throw new ConfigurationException("Invalid key: " +
          pairRepresentation, e);

    } catch (IOException e) {
      throw new ConfigurationException("Error reading one of key file: " +
          pairRepresentation, e);
    }
  }

  public List<InstanceTemplate> getInstanceTemplates() {
    return instanceTemplates;
  }

  public InstanceTemplate getInstanceTemplate(final Set<String> roles) {
    for (InstanceTemplate template : instanceTemplates) {
      if (roles.equals(template.getRoles())) {
        return template;
      }
    }
    return null;
  }

  public InstanceTemplate getInstanceTemplate(String... roles) {
    return getInstanceTemplate(Sets.newLinkedHashSet(Lists.newArrayList(roles)));
  }

  public int getMaxStartupRetries() {
    return maxStartupRetries;
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

  public String getBlobStoreProvider() {
    if (blobStoreProvider == null) {
      return getDefaultBlobStoreForComputeProvider();
    }
    return blobStoreProvider;
  }

  /**
   * Probably jclouds should provide a similar mechanism
   */
  private String getDefaultBlobStoreForComputeProvider() {
    Map<String, String> mappings = Maps.newHashMap();

    mappings.put("ec2","aws-s3");
    mappings.put("aws-ec2", "aws-s3");

    mappings.put("cloudservers", "cloudfiles-us");
    mappings.put("cloudservers-us", "cloudfiles-us");
    mappings.put("cloudservers-uk", "cloudfiles-uk");

    if (!mappings.containsKey(provider)) {
      return null;
    }
    return mappings.get(provider);
  }

  public String getBlobStoreIdentity() {
    if (blobStoreIdentity == null) {
      return identity;
    }
    return blobStoreIdentity;
  }

  public String getBlobStoreCredential() {
    if (blobStoreCredential == null) {
      return credential;
    }
    return blobStoreCredential;
  }

  public String getBlobStoreLocationId() {
    return blobStoreLocationId;
  }

  public String getServiceName() {
    return serviceName;
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

  public int getHardwareMinRam() {
    return hardwareMinRam;
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

  public String getClusterUser() {
    return clusterUser;
  }

  public String getLoginUser() {
    return loginUser;
  }

  public void setInstanceTemplates(List<InstanceTemplate> instanceTemplates) {
    this.instanceTemplates = instanceTemplates;
  }

  public void setMaxStartupRetries(int maxStartupRetries) {
    this.maxStartupRetries = maxStartupRetries;
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

  public void setBlobStoreProvider(String provider) {
    blobStoreProvider = provider;
  }

  public void setBlobStoreIdentity(String identity) {
    blobStoreIdentity = identity;
  }

  public void setBlobStoreCredential(String credential) {
    blobStoreCredential = credential;
  }

  public void setBlobStoreLocationId(String locationId) {
    blobStoreLocationId = locationId;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
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

  public void setHardwareMinRam(int minRam) {
    this.hardwareMinRam = minRam;
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

  public void setClusterUser(String user) {
    this.clusterUser = user;
  }

  public void setLoginUser(String user) {
    loginUser = config.getString(Property.LOGIN_USER.getConfigName());
    if (loginUser != null) {
      // patch until jclouds 1.0-beta-10
      System.setProperty("whirr.login-user", loginUser);
    }
  }

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
        && Objects.equal(maxStartupRetries, that.maxStartupRetries)
        && Objects.equal(provider, that.provider)
        && Objects.equal(identity, that.identity)
        && Objects.equal(credential, that.credential)
        && Objects.equal(blobStoreProvider, that.blobStoreProvider)
        && Objects.equal(blobStoreIdentity, that.blobStoreIdentity)
        && Objects.equal(blobStoreCredential, that.blobStoreCredential)
        && Objects.equal(clusterName, that.clusterName)
        && Objects.equal(serviceName, that.serviceName)
        && Objects.equal(clusterUser, that.clusterUser)
        && Objects.equal(loginUser, that.loginUser)
        && Objects.equal(imageId, that.imageId)
        && Objects.equal(hardwareId, that.hardwareId)
        && Objects.equal(hardwareMinRam, that.hardwareMinRam)
        && Objects.equal(locationId, that.locationId)
        && Objects.equal(blobStoreLocationId, that.blobStoreLocationId)
        && Objects.equal(clientCidrs, that.clientCidrs)
        && Objects.equal(version, that.version)
        ;
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hashCode(instanceTemplates, maxStartupRetries, provider,
      identity, credential, blobStoreProvider, blobStoreIdentity, blobStoreCredential,
      clusterName, serviceName, clusterUser, loginUser, publicKey, privateKey, imageId,
      hardwareId, locationId, blobStoreLocationId, clientCidrs, version, runUrlBase);
  }
  
  public String toString() {
    return Objects.toStringHelper(this)
      .add("instanceTemplates", instanceTemplates)
      .add("maxStartupRetries", maxStartupRetries)
      .add("provider", provider)
      .add("identity", identity)
      .add("credential", credential)
      .add("blobStoreProvider", blobStoreProvider)
      .add("blobStoreCredential", blobStoreCredential)
      .add("blobStoreIdentity", blobStoreIdentity)
      .add("clusterName", clusterName)
      .add("serviceName", serviceName)
      .add("clusterUser", clusterUser)
      .add("loginUser", loginUser)
      .add("publicKey", publicKey)
      .add("privateKey", privateKey)
      .add("imageId", imageId)
      .add("instanceSizeId", hardwareId)
      .add("instanceMinRam", hardwareMinRam)
      .add("locationId", locationId)
      .add("blobStoreLocationId", blobStoreLocationId)
      .add("clientCidrs", clientCidrs)
      .add("version", version)
      .toString();
  }
}
