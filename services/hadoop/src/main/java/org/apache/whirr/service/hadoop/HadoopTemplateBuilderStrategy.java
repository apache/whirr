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

package org.apache.whirr.service.hadoop;

import org.apache.whirr.service.ClusterSpec;
import org.apache.whirr.service.jclouds.TemplateBuilderStrategy;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.ec2.domain.InstanceType;

public class HadoopTemplateBuilderStrategy extends TemplateBuilderStrategy {

  public void configureTemplateBuilder(ClusterSpec clusterSpec,
      TemplateBuilder templateBuilder) {
    super.configureTemplateBuilder(clusterSpec, templateBuilder);
    
    if (clusterSpec.getProvider().equals("aws-ec2")
        && clusterSpec.getImageId() == null) {
      templateBuilder.osFamily(OsFamily.AMZN_LINUX);
    }
    if (clusterSpec.getProvider().endsWith("ec2")
        && clusterSpec.getHardwareId() == null) {
      // micro is too small for Hadoop (even for testing)
      templateBuilder.hardwareId(InstanceType.M1_SMALL);
    }
  }
}
