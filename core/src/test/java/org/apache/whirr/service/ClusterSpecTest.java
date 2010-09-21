package org.apache.whirr.service;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

public class ClusterSpecTest {
  
  @Test
  public void testDefaultsAreSet() throws ConfigurationException {
    ClusterSpec spec = new ClusterSpec();
    assertThat(spec.getRunUrlBase(),
        startsWith("http://whirr.s3.amazonaws.com/"));
  }

  @Test
  public void testDefaultsCanBeOverridden() throws ConfigurationException {
    Configuration conf = new PropertiesConfiguration();
    conf.setProperty(ClusterSpec.Property.RUN_URL_BASE.getConfigName(),
        "http://example.org");
    ClusterSpec spec = new ClusterSpec(conf);
    assertThat(spec.getRunUrlBase(),
        startsWith("http://example.org"));
  }

}
