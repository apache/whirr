package org.apache.whirr.service.jclouds;

import org.apache.whirr.service.ClusterSpec;
import org.jclouds.compute.domain.TemplateBuilder;

/**
 * A class to configure a {@link TemplateBuilder}.
 */
public class TemplateBuilderStrategy {

  public void configureTemplateBuilder(ClusterSpec clusterSpec,
      TemplateBuilder templateBuilder) {
    
    if (clusterSpec.getImageId() != null) {
      templateBuilder.imageId(clusterSpec.getImageId());
    }
    
    if (clusterSpec.getHardwareId() != null) {
      templateBuilder.hardwareId(clusterSpec.getHardwareId());
    }
    
    if (clusterSpec.getLocationId() != null) {
      templateBuilder.locationId(clusterSpec.getLocationId());
    }
  }
}
