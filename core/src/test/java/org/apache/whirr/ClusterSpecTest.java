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

import static com.google.common.collect.Iterables.get;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.util.KeyPair;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jcraft.jsch.JSchException;

public class ClusterSpecTest {

  @Test
  public void testDefaultsAreSet()
    throws ConfigurationException, JSchException, IOException {
    ClusterSpec spec = ClusterSpec.withTemporaryKeys();
    assertThat(spec.getClusterUser(),
      is(System.getProperty("user.name")));
    assertThat(spec.getMaxStartupRetries(), is(1));
  }

  @Test
  public void testDefaultsCanBeOverridden()
    throws ConfigurationException, JSchException, IOException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.RUN_URL_BASE.getConfigName(),
      "http://example.org");
    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    assertThat(spec.getRunUrlBase(), is("http://example.org"));
  }
  
  @Test
  public void testAwsEc2SpotPrice() throws ConfigurationException {
    assertEquals(ClusterSpec.withNoDefaults(new PropertiesConfiguration()).getAwsEc2SpotPrice(), null);
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.AWS_EC2_SPOT_PRICE.getConfigName(), "0.30");
    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    assertEquals(spec.getAwsEc2SpotPrice(), new Float(0.3));
  }
  
  /**
   * @see ConfigToTemplateBuilderSpecTest for more
   */
  @Test
  public void testTemplateOverrides() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true,minRam=2048");
    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    assertEquals(spec.getTemplate(), TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048"));
  }
  
  /**
   * @see ConfigToTemplateBuilderSpecTest for more
   */
  @Test
  public void testNoTemplateSetsUbuntu1004With1GB() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    assertEquals(spec.getTemplate(), TemplateBuilderSpec.parse("osFamily=UBUNTU,osVersionMatches=10.04,minRam=1024"));
  }
  
  @Test
  public void testEndpoint() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.ENDPOINT.getConfigName(), "http://compute");
    conf.setProperty(ClusterSpec.Property.BLOBSTORE_ENDPOINT.getConfigName(), "http://blobstore");
    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    assertThat(spec.getEndpoint(), is("http://compute"));
    assertThat(spec.getBlobStoreEndpoint(), is("http://blobstore"));
  }

  @Test
  public void testGetConfigurationForKeysWithPrefix()
    throws ConfigurationException, JSchException, IOException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("a.b", 1);
    conf.setProperty("b.a", 2);
    conf.setProperty("a.c", 3);

    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    Configuration prefixConf = spec.getConfigurationForKeysWithPrefix("a");

    List<String> prefixKeys = Lists.newArrayList();
    Iterators.addAll(prefixKeys, prefixConf.getKeys());

    assertThat(prefixKeys.size(), is(2));
    assertThat(prefixKeys.get(0), is("a.b"));
    assertThat(prefixKeys.get(1), is("a.c"));
  }

  @Test
  public void testEnvVariableInterpolation() {
    Map<String, String> envMap = System.getenv();
    assertThat(envMap.isEmpty(), is(false));

    String undefinedEnvVar = "UNDEFINED_ENV_VAR";
    assertThat(envMap.containsKey(undefinedEnvVar), is(false));

    Entry<String, String> firstEntry = Iterables.get(envMap.entrySet(), 0);
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("a", String.format("${env:%s}", firstEntry.getKey()));
    conf.setProperty("b", String.format("${env:%s}", undefinedEnvVar));

    assertThat(conf.getString("a"), is(firstEntry.getValue()));
    assertThat(conf.getString("b"),
      is(String.format("${env:%s}", undefinedEnvVar)));
  }

  @Test
  public void testDefaultPublicKey()
    throws ConfigurationException, JSchException, IOException {
    Map<String, File> keys = KeyPair.generateTemporaryFiles();

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.private-key-file", keys.get("private").getAbsolutePath());
    // If no public-key-file is specified it should append .pub to the private-key-file

    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    Assert.assertEquals(IOUtils.toString(
      new FileReader(keys.get("public"))), spec.getPublicKey());
  }

  @Test(expected = ConfigurationException.class)
  public void testDummyPrivateKey()
    throws JSchException, IOException, ConfigurationException {
    File privateKeyFile = File.createTempFile("private", "key");
    privateKeyFile.deleteOnExit();
    Files.write(("-----BEGIN RSA PRIVATE KEY-----\n" +
      "DUMMY FILE\n" +
      "-----END RSA PRIVATE KEY-----").getBytes(), privateKeyFile);

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.private-key-file", privateKeyFile.getAbsolutePath());

    ClusterSpec.withNoDefaults(conf);
  }

  @Test(expected = ConfigurationException.class)
  public void testEncryptedPrivateKey()
    throws JSchException, IOException, ConfigurationException {
    File privateKey = KeyPair.generateTemporaryFiles("dummy").get("private");

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.private-key-file", privateKey.getAbsolutePath());

    ClusterSpec.withNoDefaults(conf);
  }

  @Test(expected = ConfigurationException.class)
  public void testMissingPrivateKey() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.private-key-file", "/dummy/path/that/does/not/exists");

    ClusterSpec.withNoDefaults(conf);
  }

  @Test(expected = ConfigurationException.class)
  public void testMissingPublicKey() throws JSchException, IOException, ConfigurationException {
    File privateKey = KeyPair.generateTemporaryFiles().get("private");

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.private-key-file", privateKey.getAbsolutePath());
    conf.setProperty("whirr.public-key-file", "/dummy/path/that/does/not/exists");

    ClusterSpec.withNoDefaults(conf);
  }

  @Test(expected = ConfigurationException.class)
  public void testBrokenPublicKey() throws IOException, JSchException, ConfigurationException {
    File privateKey = KeyPair.generateTemporaryFiles().get("private");

    File publicKey = File.createTempFile("public", "key");
    publicKey.deleteOnExit();
    Files.write("ssh-rsa BROKEN PUBLIC KEY".getBytes(), publicKey);

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.private-key-file", privateKey.getAbsolutePath());
    conf.setProperty("whirr.public-key-file", publicKey.getAbsolutePath());

    ClusterSpec.withNoDefaults(conf);
  }

  @Test(expected = ConfigurationException.class)
  public void testNotSameKeyPair() throws JSchException, IOException, ConfigurationException {
    Map<String, File> first = KeyPair.generateTemporaryFiles();
    Map<String, File> second = KeyPair.generateTemporaryFiles();

    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.private-key-file", first.get("private").getAbsolutePath());
    conf.setProperty("whirr.public-key-file", second.get("public").getAbsolutePath());

    ClusterSpec.withNoDefaults(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingCommaInInstanceTemplates() throws Exception {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.INSTANCE_TEMPLATES.getConfigName(),
      "1 a+b 2 c+d"); // missing comma
    ClusterSpec.withTemporaryKeys(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRoleMayNotContainSpaces() {
    InstanceTemplate.builder()
      .numberOfInstance(1)
      .minNumberOfInstances(1)
      .roles("a b").build();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIllegalArgumentExceptionOnInstancesTemplates() throws Exception {
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.instance-templates", "1 hadoop-namenode+hadoop-jobtracker,3 hadoop-datanode+hadoop-tasktracker");
    conf.addProperty("whirr.instance-templates-max-percent-failures", "60 % hadoop-datanode+hadoop-tasktracker");
    ClusterSpec expectedClusterSpec = ClusterSpec.withNoDefaults(conf);
    List<InstanceTemplate> templates = expectedClusterSpec.getInstanceTemplates();
    InstanceTemplate t1 = templates.get(0);
    assertThat(t1.getMinNumberOfInstances(), is(1));
    InstanceTemplate t2 = templates.get(1);
    assertThat(t2.getMinNumberOfInstances(), is(2));
  }

  @Test(expected = NumberFormatException.class)
  public void testNumberFormatExceptionOnInstancesTemplates() throws Exception {
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.instance-templates", "1 hadoop-namenode+hadoop-jobtracker,3 hadoop-datanode+hadoop-tasktracker");
    conf.addProperty("whirr.instance-templates-max-percent-failures", "60% hadoop-datanode+hadoop-tasktracker");
    ClusterSpec expectedClusterSpec = ClusterSpec.withNoDefaults(conf);
    List<InstanceTemplate> templates = expectedClusterSpec.getInstanceTemplates();
    InstanceTemplate t1 = templates.get(0);
    assertThat(t1.getMinNumberOfInstances(), is(1));
    InstanceTemplate t2 = templates.get(1);
    assertThat(t2.getMinNumberOfInstances(), is(2));
  }

  @Test
  public void testNumberOfInstancesPerTemplate() throws Exception {
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.instance-templates", "1 hadoop-namenode+hadoop-jobtracker,3 hadoop-datanode+hadoop-tasktracker");
    conf.addProperty("whirr.instance-templates-max-percent-failures", "100 hadoop-namenode+hadoop-jobtracker,60 hadoop-datanode+hadoop-tasktracker");
    ClusterSpec expectedClusterSpec = ClusterSpec.withNoDefaults(conf);
    List<InstanceTemplate> templates = expectedClusterSpec.getInstanceTemplates();
    InstanceTemplate t1 = templates.get(0);
    assertThat(t1.getMinNumberOfInstances(), is(1));
    InstanceTemplate t2 = templates.get(1);
    assertThat(t2.getMinNumberOfInstances(), is(2));

    conf.setProperty("whirr.instance-templates-max-percent-failures", "60 hadoop-datanode+hadoop-tasktracker");
    expectedClusterSpec = ClusterSpec.withNoDefaults(conf);
    templates = expectedClusterSpec.getInstanceTemplates();
    t1 = templates.get(0);
    assertThat(t1.getMinNumberOfInstances(), is(1));
    t2 = templates.get(1);
    assertThat(t2.getMinNumberOfInstances(), is(2));

    conf.addProperty("whirr.instance-templates-minumum-number-of-instances", "1 hadoop-datanode+hadoop-tasktracker");
    expectedClusterSpec = ClusterSpec.withNoDefaults(conf);
    templates = expectedClusterSpec.getInstanceTemplates();
    t1 = templates.get(0);
    assertThat(t1.getMinNumberOfInstances(), is(1));
    t2 = templates.get(1);
    assertThat(t2.getMinNumberOfInstances(), is(2));

    conf.setProperty("whirr.instance-templates-minimum-number-of-instances", "3 hadoop-datanode+hadoop-tasktracker");
    expectedClusterSpec = ClusterSpec.withNoDefaults(conf);
    templates = expectedClusterSpec.getInstanceTemplates();
    t1 = templates.get(0);
    assertThat(t1.getMinNumberOfInstances(), is(1));
    t2 = templates.get(1);
    assertThat(t2.getMinNumberOfInstances(), is(3));
  }

  @Test
  public void testClusterUserShouldBeCurrentUser() throws Exception {
    ClusterSpec spec = ClusterSpec.withTemporaryKeys();
    assertThat(spec.getClusterUser(), is(System.getProperty("user.name")));
  }

  @Test
  public void testDefaultBlobStoreforComputeProvider() throws Exception {
    for (String pair : new String[]{
      "ec2:aws-s3",
      "aws-ec2:aws-s3",
      "cloudservers:cloudfiles-us",
      "cloudservers-us:cloudfiles-us",
      "cloudservers-uk:cloudfiles-uk"
    }) {
      String[] parts = pair.split(":");

      Configuration config = new PropertiesConfiguration();
      config.addProperty("whirr.provider", parts[0]);

      ClusterSpec spec = ClusterSpec.withTemporaryKeys(config);
      assertThat(spec.getBlobStoreProvider(), is(parts[1]));
    }
  }

  @Test
  public void testAutoHostnameForProvider() throws Exception {
      Configuration cloudServersConfig = new PropertiesConfiguration();
      cloudServersConfig.addProperty("whirr.provider", "cloudservers-us");

      ClusterSpec cloudServersSpec = ClusterSpec.withTemporaryKeys(cloudServersConfig);
      assertEquals(cloudServersSpec.getAutoHostnamePrefix(), null);
      assertEquals(cloudServersSpec.getAutoHostnameSuffix(), ".static.cloud-ips.com");

      Configuration cloudServersUkConfig = new PropertiesConfiguration();
      cloudServersUkConfig.addProperty("whirr.provider", "cloudservers-uk");

      ClusterSpec cloudServersUkSpec = ClusterSpec.withTemporaryKeys(cloudServersUkConfig);
      assertEquals(cloudServersUkSpec.getAutoHostnamePrefix(), null);
      assertEquals(cloudServersUkSpec.getAutoHostnameSuffix(), ".static.cloud-ips.co.uk");

      Configuration ec2Config = new PropertiesConfiguration();
      ec2Config.addProperty("whirr.provider", "aws-ec2");

      ClusterSpec ec2Spec = ClusterSpec.withTemporaryKeys(ec2Config);
      assertEquals(null, ec2Spec.getAutoHostnamePrefix());
      assertEquals(null, ec2Spec.getAutoHostnameSuffix());
  }

  @Test
  public void testJdkInstallUrl() throws Exception {
      Configuration cloudServersConfig = new PropertiesConfiguration();

      ClusterSpec cloudServersSpec = ClusterSpec.withTemporaryKeys(cloudServersConfig);
      assertEquals(null, cloudServersSpec.getJdkInstallUrl());

      cloudServersConfig = new PropertiesConfiguration();
      cloudServersConfig.addProperty("whirr.jdk-install-url", "http://whirr-third-party.s3.amazonaws.com/jdk-6u21-linux-i586-rpm.bin");

      cloudServersSpec = ClusterSpec.withTemporaryKeys(cloudServersConfig);
      assertEquals("http://whirr-third-party.s3.amazonaws.com/jdk-6u21-linux-i586-rpm.bin", cloudServersSpec.getJdkInstallUrl());
  }  
  

  @Test
  public void testApplySubroleAliases() throws ConfigurationException {
    CompositeConfiguration c = new CompositeConfiguration();
    Configuration config = new PropertiesConfiguration();
    config.addProperty("whirr.instance-templates",
      "1 puppet:somepup::pet+something-else, 1 something-else-only");
    c.addConfiguration(config);
    InstanceTemplate template = InstanceTemplate.parse(c).get(0);
    Set<String> expected = Sets.newLinkedHashSet(Arrays.asList(new String[]{
      "puppet:somepup::pet", "something-else"}));
    assertThat(template.getRoles(), is(expected));

    InstanceTemplate template2 = InstanceTemplate.parse(c).get(1);
    Set<String> expected2 = Sets.newLinkedHashSet(Arrays.asList(new String[]{
      "something-else-only"}));
    assertThat(template2.getRoles(), is(expected2));
  }

  @Test
  public void testCopySpec() throws Exception {
    ClusterSpec spec = ClusterSpec.withTemporaryKeys(
      new PropertiesConfiguration("whirr-core-test.properties"));
    spec.setTemplate(TemplateBuilderSpec.parse("locationId=random-location"));

    /* check the copy is the same as the original */
    assertThat(spec.copy(), is(spec));
    assertThat(spec.copy().hashCode(), is(spec.hashCode()));
  }

  @Test
  public void testFirewallRules() throws Exception {
    PropertiesConfiguration conf = new PropertiesConfiguration("whirr-core-test.properties");
    conf.setProperty("whirr.firewall-rules", "8000,8001");
    conf.setProperty("whirr.firewall-rules.serviceA", "9000,9001");
    ClusterSpec spec = ClusterSpec.withTemporaryKeys(conf);

    Map<String, List<String>> firewallRules = spec.getFirewallRules();
    assertThat(firewallRules.get(null).equals(Lists.<String>newArrayList("8000", "8001")), is(true));
    assertThat(firewallRules.get("serviceA").equals(Lists.<String>newArrayList("9000", "9001")), is(true));
  }

  @Test
  public void testHardwareIdPerInstanceTemplate() throws Exception {
    PropertiesConfiguration conf = new PropertiesConfiguration("whirr-core-test.properties");
    conf.setProperty("whirr.instance-templates", "2 noop, 1 role1+role2, 1 role1, 3 spots, 1 spec");
    conf.setProperty("whirr.hardware-id", "c1.xlarge");

    conf.setProperty("whirr.templates.noop.hardware-id", "m1.large");
    conf.setProperty("whirr.templates.role1+role2.hardware-id", "t1.micro");
    conf.setProperty("whirr.templates.role1+role2.image-id", "us-east-1/ami-123324");
    conf.setProperty("whirr.templates.spots.aws-ec2-spot-price", 0.5f);
    conf.setProperty("whirr.templates.spec.template", "osFamily=UBUNTU,os64Bit=true,minRam=2048");

    ClusterSpec spec = ClusterSpec.withTemporaryKeys(conf);
    List<InstanceTemplate> templates = spec.getInstanceTemplates();

    InstanceTemplate noops = get(templates, 0);
    assert noops.getRoles().contains("noop");
    assertEquals(noops.getTemplate().getHardwareId(), "m1.large");
    assertEquals(noops.getTemplate().getImageId(), null);
    assertEquals(noops.getAwsEc2SpotPrice(), null);

    InstanceTemplate second = get(templates, 1);
    assertEquals(second.getTemplate().getHardwareId(), "t1.micro");
    assertEquals(second.getTemplate().getImageId(), "us-east-1/ami-123324");
    assertEquals(second.getAwsEc2SpotPrice(), null);

    InstanceTemplate third = get(templates, 2);
    assertEquals(third.getTemplate(), null);
    assertEquals(third.getAwsEc2SpotPrice(), null);

    InstanceTemplate spots = get(templates, 3);
    assertEquals(spots.getAwsEc2SpotPrice(), new Float(0.5f), 0.001);

    InstanceTemplate template = get(templates, 4);
    assertEquals(template.getTemplate(), TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048"));
    assertEquals(template.getAwsEc2SpotPrice(), null);

  }

  @Test(expected = ConfigurationException.class)
  public void testInstanceTemplateNotFoundForHardwareId() throws Exception {
    PropertiesConfiguration conf = new PropertiesConfiguration("whirr-core-test.properties");
    conf.setProperty("whirr.instance-templates", "1 role1+role2");
    conf.setProperty("whirr.templates.role1.hardware-id", "m1.large");

    ClusterSpec.withTemporaryKeys(conf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailIfRunningAsRootOrClusterUserIsRoot() throws ConfigurationException {
    PropertiesConfiguration conf = new PropertiesConfiguration("whirr-core-test.properties");
    conf.setProperty("whirr.cluster-user", "root");

    ClusterSpec.withNoDefaults(conf);
  }
}
