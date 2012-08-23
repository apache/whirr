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

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.Iterables.any;

import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.ClusterSpec.Property;
import org.jclouds.compute.domain.TemplateBuilderSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;

/**
 * Takes configuration parameters and converts them to templateBuilderSpec
 */
public enum ConfigToTemplateBuilderSpec implements Function<Configuration, TemplateBuilderSpec> {
  INSTANCE;

  static final Logger LOG = LoggerFactory.getLogger(ClusterSpec.class);

  @Override
  public TemplateBuilderSpec apply(Configuration in) {
    // until TemplateBuilderSpec has type-safe builder
    Builder<String, String> template = ImmutableMap.<String, String> builder();
    if (in.getList(Property.TEMPLATE.getConfigName()).size() > 0) {
      template.putAll(Splitter.on(',').withKeyValueSeparator("=")
            .split(Joiner.on(',').join(in.getList(Property.TEMPLATE.getConfigName()))));
    }
    if (!hasImageParams(template.build())) {
      String imageId = in.getString(Property.IMAGE_ID.getConfigName());
      if (imageId != null) {
        template.put("imageId", imageId);
      } else {
        template.put("osFamily", "UBUNTU");
        template.put("osVersionMatches", "10.04");
        // canonical images, but not testing ones
        if ("aws-ec2".equals(in.getString(Property.PROVIDER.getConfigName())))
          template.put("osDescriptionMatches", "^(?!.*(daily|testing)).*ubuntu-images.*$");
      }
    }
    if (!hasHardwareParams(template.build())) {
      String hardwareId = in.getString(Property.HARDWARE_ID.getConfigName());
      if (hardwareId != null) {
        template.put("hardwareId", hardwareId);
      } else {
        template.put("minRam", Integer.toString(in.getInt(Property.HARDWARE_MIN_RAM.getConfigName(), 1024)));
      }
    }
    if (!hasLocationParams(template.build())) {
      String locationId = in.getString(Property.LOCATION_ID.getConfigName());
      if (locationId != null) {
        template.put("locationId", locationId);
      }
    }
    if (!hasLoginParams(template.build())) {
      String bootstrapUser = getBootstrapUserOrDeprecatedLoginUser(in);
      if (bootstrapUser != null) {
        template.put("loginUser", bootstrapUser);
      }
    }
    return TemplateBuilderSpec.parse(Joiner.on(',').withKeyValueSeparator("=").join(template.build()));
  }

  private static boolean hasImageParams(Map<String, String> template) {
    return any(template.keySet(), in(ImmutableSet.of("imageId", "imageNameMatches", "osFamily", "osVersionMatches",
          "os64Bit", "osArchMatches", "osDescriptionMatches")));
  }

  private static boolean hasHardwareParams(Map<String, String> template) {
    return any(template.keySet(),
          in(ImmutableSet.of("hardwareId", "minCores", "minRam", "minDisk", "hypervisorMatches")));
  }

  private static boolean hasLocationParams(Map<String, String> template) {
    return any(template.keySet(), in(ImmutableSet.of("locationId")));
  }

  private static boolean hasLoginParams(Map<String, String> template) {
    return any(template.keySet(), in(ImmutableSet.of("loginUser", "authenticateSudo")));
  }

  private static String getBootstrapUserOrDeprecatedLoginUser(Configuration in) {
    final String loginUserConfig = "whirr.login-user";
    if (in.containsKey(loginUserConfig)) {
      LOG.warn("whirr.login-user is deprecated. Please rename to whirr.bootstrap-user.");
      return in.getString(loginUserConfig, null);
    }
    return in.getString(Property.BOOTSTRAP_USER.getConfigName());
  }
}
