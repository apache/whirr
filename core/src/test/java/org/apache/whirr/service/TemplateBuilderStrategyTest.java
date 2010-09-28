package org.apache.whirr.service;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.compute.domain.TemplateBuilder;
import org.junit.Before;
import org.junit.Test;

public class TemplateBuilderStrategyTest {
  
  private TemplateBuilderStrategy strategy = new TemplateBuilderStrategy();
  private ClusterSpec spec;
  
  @Before
  public void setUp() throws ConfigurationException {
    spec = new ClusterSpec();
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
