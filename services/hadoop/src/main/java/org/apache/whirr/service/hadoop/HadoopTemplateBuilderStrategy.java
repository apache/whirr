package org.apache.whirr.service.hadoop;

import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.aws.ec2.compute.domain.EC2Hardware;
import org.jclouds.compute.domain.TemplateBuilder;

public class HadoopTemplateBuilderStrategy extends TemplateBuilderStrategy {

  public void configureTemplateBuilder(ClusterSpec clusterSpec,
      TemplateBuilder templateBuilder) {
    super.configureTemplateBuilder(clusterSpec, templateBuilder);
    
    if ("ec2".equals(clusterSpec.getProvider())
        && clusterSpec.getHardwareId() == null) {
      // micro is too small for Hadoop (even for testing)
      templateBuilder.fromHardware(EC2Hardware.M1_SMALL);
    }
  }
}
