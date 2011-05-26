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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.jcraft.jsch.JSchException;

import java.io.IOException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.compute.domain.TemplateBuilder;
import org.junit.Before;
import org.junit.Test;

public class TemplateBuilderStrategyTest {
  
  private TemplateBuilderStrategy strategy = new TemplateBuilderStrategy();
  private ClusterSpec spec;
  
  @Before
  public void setUp() throws ConfigurationException, JSchException, IOException {
    spec = ClusterSpec.withTemporaryKeys();
  }
  
  @Test
  public void testImageIdIsPassedThrough() {
    spec.setImageId("my-image-id");
    TemplateBuilder builder = mock(TemplateBuilder.class);
    strategy.configureTemplateBuilder(spec, builder);
    verify(builder).imageId("my-image-id");
  }
  
  @Test
  public void testHardwareIdIsPassedThrough() {
    spec.setHardwareId("my-hardware-id");
    TemplateBuilder builder = mock(TemplateBuilder.class);
    strategy.configureTemplateBuilder(spec, builder);
    verify(builder).hardwareId("my-hardware-id");
  }

  @Test
  public void testLocationIdIsPassedThrough() {
    spec.setLocationId("my-location-id");
    TemplateBuilder builder = mock(TemplateBuilder.class);
    strategy.configureTemplateBuilder(spec, builder);
    verify(builder).locationId("my-location-id");
  }

}
