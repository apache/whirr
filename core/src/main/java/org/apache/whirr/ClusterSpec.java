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
import org.jclouds.predicates.validators.DnsNameValidator;
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

    BLOBSTORE_CACHE_CONTAINER(String.class, false, "The name of the " +
        "container to be used for caching local files. If not specified Whirr will " +
        "create a random one and remove it at the end of the session."),

    STATE_STORE(String.class, false, "What kind of store to use for state " +
      "(local, blob or none). Defaults to local."),

    STATE_STORE_CONTAINER(String.class, false, "Container where to store state. " +
      "Valid only for the blob state store."),

    STATE_STORE_BLOB(String.class, false, "Blob name for state storage. " +
      "Valid only for the blob state store. Defaults to whirr-<cluster-name>"),

    AWS_EC2_SPOT_PRICE(Float.class, false, "Spot instance price (aws-ec2 specific option)"),
      
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
  private String blobStoreCacheContainer;

  private String stateStore;
  private String stateStoreContainer;
  private String stateStoreBlob;

  private float awsEc2SpotPrice;

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
    setBlobStoreCacheContainer(getString(Property.BLOBSTORE_CACHE_CONTAINER));

    setStateStore(getString(Property.STATE_STORE));
    setStateStoreContainer(getString(Property.STATE_STORE_CONTAINER));
    setStateStoreBlob(getString(Property.STATE_STORE_BLOB));

    setAwsEc2SpotPrice(getFloat(Property.AWS_EC2_SPOT_PRICE, -1));

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

  /**
   * Create a deep object copy. It's not enough to just copy the configuration
   * because the object can also be modified using the setters and the changes
   * are not reflected in the configuration object.
   */
  public ClusterSpec copy() throws ConfigurationException {
    ClusterSpec r = new ClusterSpec(getConfiguration(), true);

    r.setClusterName(getClusterName());
    r.setServiceName(getServiceName());

    r.setLoginUser(getLoginUser());
    r.setClusterUser(getClusterUser());

    r.setInstanceTemplates(Lists.newLinkedList(getInstanceTemplates()));
    r.setMaxStartupRetries(getMaxStartupRetries());

    r.setProvider(getProvider());
    r.setIdentity(getIdentity());
    r.setCredential(getCredential());

    r.setBlobStoreProvider(getBlobStoreProvider());
    r.setBlobStoreIdentity(getBlobStoreIdentity());
    r.setBlobStoreCredential(getBlobStoreCredential());
    r.setBlobStoreCacheContainer(getBlobStoreCacheContainer());

    r.setAwsEc2SpotPrice(getAwsEc2SpotPrice());

    r.setStateStore(getStateStore());
    r.setStateStoreContainer(getStateStoreContainer());
    r.setStateStoreBlob(getStateStoreBlob());

    r.setPrivateKey(getPrivateKey());
    r.setPublicKey(getPublicKey());

    r.setImageId(getImageId());
    r.setHardwareId(getHardwareId());
    r.setHardwareMinRam(getHardwareMinRam());

    r.setLocationId(getLocationId());
    r.setBlobStoreLocationId(getBlobStoreLocationId());
    r.setClientCidrs(getClientCidrs());

    r.setVersion(getVersion());
    r.setRunUrlBase(getRunUrlBase());

    return r;
  }

  private String getString(Property key) {
    return config.getString(key.getConfigName(), null);
  }

  private int getInt(Property key, int defaultValue) {
    return config.getInt(key.getConfigName(), defaultValue);
  }

  private float getFloat(Property key, float defaultValue) {
    return config.getFloat(key.getConfigName(), defaultValue);
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

  public String getBlobStoreCacheContainer() {
    return blobStoreCacheContainer;
  }

  public String getStateStore() {
    if (stateStore == null) {
      return "local";
    }
    return stateStore;
  }

  public String getStateStoreContainer() {
    return stateStoreContainer;
  }

  public String getStateStoreBlob() {
    if (stateStoreBlob == null && "blob".equals(stateStore)) {
      return "whirr-" + getClusterName();
    }
    return stateStoreBlob;
  }

  public float getAwsEc2SpotPrice() {
    return awsEc2SpotPrice;
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
    if ("ec2".equals(provider)) {
      LOG.warn("Please use provider \"aws-ec2\" instead of \"ec2\"");
      provider = "aws-ec2";
    }
    if ("cloudservers".equals(provider)) {
      LOG.warn("Please use provider \"cloudservers-us\" instead of \"cloudservers\"");
      provider = "cloudservers-us";
    }
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

  public void setBlobStoreCacheContainer(String container) {
    blobStoreCacheContainer = container;
  }

  public void setStateStore(String type) {
    if (type != null) {
      checkArgument(Sets.newHashSet("local", "blob", "none").contains(type),
        "Invalid state store. Valid values are local, blob or none.");
    }
    this.stateStore = type;
  }

  public void setStateStoreContainer(String container) {
    checkContainerName(container);
    this.stateStoreContainer = container;
  }

  private void checkContainerName(String name) {
    if (name != null) {
      checkArgument((new DnsNameValidator(3, 63){}).apply(name));
    }
  }

  public void setStateStoreBlob(String blob) {
    this.stateStoreBlob = blob;
  }

  public void setAwsEc2SpotPrice(float value) {
    this.awsEc2SpotPrice = value;
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
      return Objects.equal(getInstanceTemplates(), that.getInstanceTemplates())
        && Objects.equal(getMaxStartupRetries(), that.getMaxStartupRetries())
        && Objects.equal(getProvider(), that.getProvider())
        && Objects.equal(getIdentity(), that.getIdentity())
        && Objects.equal(getCredential(), that.getCredential())
        && Objects.equal(getBlobStoreProvider(), that.getBlobStoreProvider())
        && Objects.equal(getBlobStoreIdentity(), that.getBlobStoreIdentity())
        && Objects.equal(getBlobStoreCredential(), that.getBlobStoreCredential())
        && Objects.equal(getBlobStoreCacheContainer(), that.getBlobStoreCacheContainer())
        && Objects.equal(getClusterName(), that.getClusterName())
        && Objects.equal(getServiceName(), that.getServiceName())
        && Objects.equal(getClusterUser(), that.getClusterUser())
        && Objects.equal(getLoginUser(), that.getLoginUser())
        && Objects.equal(getPublicKey(), that.getPublicKey())
        && Objects.equal(getPrivateKey(), that.getPrivateKey())
        && Objects.equal(getImageId(), that.getImageId())
        && Objects.equal(getHardwareId(), that.getHardwareId())
        && Objects.equal(getHardwareMinRam(), that.getHardwareMinRam())
        && Objects.equal(getLocationId(), that.getLocationId())
        && Objects.equal(getBlobStoreLocationId(), that.getBlobStoreLocationId())
        && Objects.equal(getClientCidrs(), that.getClientCidrs())
        && Objects.equal(getVersion(), that.getVersion())
        && Objects.equal(getRunUrlBase(), that.getRunUrlBase())
        && Objects.equal(getStateStore(), that.getStateStore())
        && Objects.equal(getStateStoreContainer(), that.getStateStoreContainer())
        && Objects.equal(getStateStoreBlob(), that.getStateStoreBlob())
        && Objects.equal(getAwsEc2SpotPrice(), that.getAwsEc2SpotPrice())
        ;
    }
    return false;
  }
  
  public int hashCode() {
    return Objects.hashCode(
        getInstanceTemplates(),
        getMaxStartupRetries(),
        getProvider(),
        getIdentity(),
        getCredential(),
        getBlobStoreProvider(),
        getBlobStoreIdentity(),
        getBlobStoreCredential(),
        getBlobStoreCacheContainer(),
        getClusterName(),
        getServiceName(),
        getClusterUser(),
        getLoginUser(),
        getPublicKey(),
        getPrivateKey(),
        getImageId(),
        getHardwareId(),
        getHardwareMinRam(),
        getLocationId(),
        getBlobStoreLocationId(),
        getClientCidrs(),
        getVersion(),
        getRunUrlBase(),
        getStateStore(),
        getStateStoreBlob(),
        getStateStoreContainer(),
        getAwsEc2SpotPrice()
    );
  }
  
  public String toString() {
    return Objects.toStringHelper(this)
      .add("instanceTemplates", getInstanceTemplates())
      .add("maxStartupRetries", getMaxStartupRetries())
      .add("provider", getProvider())
      .add("identity", getIdentity())
      .add("credential", getCredential())
      .add("blobStoreProvider", getBlobStoreProvider())
      .add("blobStoreCredential", getBlobStoreCredential())
      .add("blobStoreIdentity", getBlobStoreIdentity())
      .add("blobStoreCacheContainer", getBlobStoreCacheContainer())
      .add("clusterName", getClusterName())
      .add("serviceName", getServiceName())
      .add("clusterUser", getClusterUser())
      .add("loginUser", getLoginUser())
      .add("publicKey", getPublicKey())
      .add("privateKey", getPrivateKey())
      .add("imageId", getImageId())
      .add("hardwareId", getHardwareId())
      .add("hardwareMinRam", getHardwareMinRam())
      .add("locationId", getLocationId())
      .add("blobStoreLocationId", getBlobStoreLocationId())
      .add("clientCidrs", getClientCidrs())
      .add("version", getVersion())
      .add("runUrlBase", getRunUrlBase())
      .add("stateStore", getStateStore())
      .add("stateStoreContainer", getStateStoreContainer())
      .add("stateStoreBlob", getStateStoreBlob())
      .add("awsEc2SpotPrice", getAwsEc2SpotPrice())
      .toString();
  }
}
