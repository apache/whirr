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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.jcraft.jsch.JSchException;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.IOUtils;
import org.apache.whirr.service.ClusterSpec.InstanceTemplate;
import org.apache.whirr.ssh.KeyPair;
import org.junit.Assert;
import org.junit.Test;

public class ClusterSpecTest {
  
  @Test
  public void testDefaultsAreSet()
  throws ConfigurationException, JSchException, IOException {
    ClusterSpec spec = ClusterSpec.withTemporaryKeys();
    assertThat(spec.getRunUrlBase(),
        startsWith("http://whirr.s3.amazonaws.com/"));
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
  public void testVersionInRunUrlbaseIsUrlEncoded()
  throws ConfigurationException, JSchException, IOException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.VERSION.getConfigName(), "0.1.0+1");
    ClusterSpec spec = ClusterSpec.withNoDefaults(conf);
    assertThat(spec.getRunUrlBase(),
        is("http://whirr.s3.amazonaws.com/0.1.0%2B1/"));
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
    new InstanceTemplate(1, "a b");
  }

  @Test
  public void testApplyRoleAliases() {
    InstanceTemplate template = InstanceTemplate.parse("1 nn+jt+tt+dn+zk").get(0);
    Set<String> expected = Sets.newLinkedHashSet(Arrays.asList(new String[]{
        "hadoop-namenode", "hadoop-jobtracker", "hadoop-tasktracker",
        "hadoop-datanode", "zookeeper"}));
    assertThat(template.getRoles(), is(expected));
  }

}
