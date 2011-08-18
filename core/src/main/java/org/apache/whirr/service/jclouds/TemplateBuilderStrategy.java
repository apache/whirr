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

package org.apache.whirr.service.jclouds;

import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.TemplateBuilder;

/**
 * A class to configure a {@link TemplateBuilder}.
 */
public class TemplateBuilderStrategy {

  public void configureTemplateBuilder(ClusterSpec clusterSpec,
      TemplateBuilder templateBuilder) {

    if (clusterSpec.getImageId() != null) {
      templateBuilder.imageId(clusterSpec.getImageId());
    } else {
      templateBuilder.os64Bit(true);
      templateBuilder.osFamily(OsFamily.UBUNTU);
      templateBuilder.osVersionMatches("10.04");

      // canonical images, but not testing ones
      if ("aws-ec2".equals(clusterSpec.getProvider()))
        templateBuilder.imageDescriptionMatches("/ubuntu-images/");
    }
    
    if (clusterSpec.getHardwareId() != null) {
      templateBuilder.hardwareId(clusterSpec.getHardwareId());
    } else if(clusterSpec.getHardwareMinRam() != 0) {
      templateBuilder.minRam(clusterSpec.getHardwareMinRam());
    } else {
      templateBuilder.minRam(1024);
    }

    if (clusterSpec.getLocationId() != null) {
      templateBuilder.locationId(clusterSpec.getLocationId());
    }
  }
}
