/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.internal;

import static org.junit.Assert.assertEquals;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.junit.Test;

public class ConfigToTemplateBuilderSpecTest {

  @Test
  public void testTemplate() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true,minRam=2048");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048"));
  }

  @Test
  public void testTemplateWithHardwareSpecsOverridesHardwareId() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true,minRam=2048");
    conf.setProperty(ClusterSpec.Property.HARDWARE_ID.getConfigName(), "m1.small");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048"));
  }

  @Test
  public void testTemplateWithoutHardwareSpecsAcceptsHardwareId() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true");
    conf.setProperty(ClusterSpec.Property.HARDWARE_ID.getConfigName(), "m1.small");

    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,hardwareId=m1.small"));
  }

  @Test
  public void testTemplateWithImageSpecsOverridesImageId() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true,minRam=2048");
    conf.setProperty(ClusterSpec.Property.IMAGE_ID.getConfigName(), "ami-fooo");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048"));
  }

  @Test
  public void testTemplateWithoutImageSpecsAcceptsImageId() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "minRam=2048");
    conf.setProperty(ClusterSpec.Property.IMAGE_ID.getConfigName(), "ami-fooo");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("minRam=2048,imageId=ami-fooo"));
  }

  @Test
  public void testTemplateWithoutImageSpecsDefaultsToUbuntu1004() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "minRam=2048");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("minRam=2048,osFamily=UBUNTU,osVersionMatches=10.04"));
  }

  @Test
  public void testTemplateWithoutImageSpecsDefaultsToUbuntu1004AndAWSPatternOnEC2() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.PROVIDER.getConfigName(), "aws-ec2");
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "minRam=2048");
    assertEquals(
          ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec
                .parse("minRam=2048,osFamily=UBUNTU,osVersionMatches=10.04,osDescriptionMatches=^(?!.*(daily|testing)).*ubuntu-images.*$"));
  }

  @Test
  public void testTemplateWithoutHardwareSpecsDefaultsTo1GB() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=1024"));
  }

  @Test
  public void testTemplateWithDeprecatedLoginUser() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty("whirr.login-user", "user:pass");
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true,minRam=2048");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048,loginUser=user:pass"));
  }

  @Test
  public void testTemplateWithLocationSpecsOverridesLocationId() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(),
          "osFamily=UBUNTU,os64Bit=true,minRam=2048,locationId=us-west-2");
    conf.setProperty(ClusterSpec.Property.LOCATION_ID.getConfigName(), "eu-west-1");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048,locationId=us-west-2"));
  }

  @Test
  public void testTemplateWithoutLocationSpecsAcceptsLocationId() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true,minRam=2048");
    conf.setProperty(ClusterSpec.Property.LOCATION_ID.getConfigName(), "eu-west-1");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048,locationId=eu-west-1"));
  }

  @Test
  public void testTemplateWithLoginSpecsOverridesBootstrapUser() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(),
          "osFamily=UBUNTU,os64Bit=true,minRam=2048,loginUser=foo");
    conf.setProperty(ClusterSpec.Property.BOOTSTRAP_USER.getConfigName(), "user:pass");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048,loginUser=foo"));
  }

  @Test
  public void testTemplateWithoutLoginSpecsAcceptsBootstrapUser() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.TEMPLATE.getConfigName(), "osFamily=UBUNTU,os64Bit=true,minRam=2048");
    conf.setProperty(ClusterSpec.Property.BOOTSTRAP_USER.getConfigName(), "user:pass");
    assertEquals(ConfigToTemplateBuilderSpec.INSTANCE.apply(conf),
          TemplateBuilderSpec.parse("osFamily=UBUNTU,os64Bit=true,minRam=2048,loginUser=user:pass"));
  }

}
