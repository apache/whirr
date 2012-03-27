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
package org.apache.whirr;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newLinkedHashSet;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.StringUtils;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * This class describes the type of instances that should be in the cluster.
 * This is done by specifying the number of instances in each role.
 */
public class InstanceTemplate {

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private int numberOfInstances = -1;
    private int minNumberOfInstances = -1;
    private String hardwareId;
    private String imageId;
    private float awsEc2SpotPrice = 0;
    private Set<String> roles;

    public Builder numberOfInstance(int numberOfInstances) {
      this.numberOfInstances = numberOfInstances;
      return this;
    }

    public Builder minNumberOfInstances(int minNumberOfInstances) {
      this.minNumberOfInstances = minNumberOfInstances;
      return this;
    }

    public Builder hardwareId(String hardwareId) {
      this.hardwareId = hardwareId;
      return this;
    }

    public Builder imageId(String imageId) {
      this.imageId = imageId;
      return this;
    }

    public Builder awsEc2SpotPrice(float awsEc2SpotPrice) {
      this.awsEc2SpotPrice = awsEc2SpotPrice;
      return this;
    }

    public Builder roles(String... roles) {
      this.roles = newLinkedHashSet(newArrayList(roles));
      return this;
    }

    public Builder roles(Set<String> roles) {
      this.roles = newLinkedHashSet(roles);
      return this;
    }

    public InstanceTemplate build() {
      if (minNumberOfInstances == -1) {
        minNumberOfInstances = numberOfInstances;
      }
      return new InstanceTemplate(numberOfInstances, minNumberOfInstances, roles,
        hardwareId, imageId, awsEc2SpotPrice);
    }
  }

  private int numberOfInstances;
  private int minNumberOfInstances;  // some instances may fail, at least a minimum number is required
  private String hardwareId;
  private String imageId;
  private float awsEc2SpotPrice;
  private Set<String> roles;


  private InstanceTemplate(int numberOfInstances, int minNumberOfInstances,
      Set<String> roles, String hardwareId, String imageId, float awsEc2SpotPrice) {
    for (String role : roles) {
      checkArgument(!StringUtils.contains(role, " "),
        "Role '%s' may not contain space characters.", role);
    }

    this.numberOfInstances = numberOfInstances;
    this.minNumberOfInstances = minNumberOfInstances;
    this.hardwareId = hardwareId;
    this.imageId = imageId;
    this.awsEc2SpotPrice = awsEc2SpotPrice;
    this.roles = roles;
  }

  public Set<String> getRoles() {
    return roles;
  }

  public int getNumberOfInstances() {
    return numberOfInstances;
  }

  public int getMinNumberOfInstances() {
    return minNumberOfInstances;
  }

  public String getHardwareId() {
    return hardwareId;
  }

  public String getImageId() {
    return imageId;
  }

  public float getAwsEc2SpotPrice() {
    return awsEc2SpotPrice;
  }

  public boolean equals(Object o) {
    if (o instanceof InstanceTemplate) {
      InstanceTemplate that = (InstanceTemplate) o;
      return numberOfInstances == that.numberOfInstances
        && minNumberOfInstances == that.minNumberOfInstances
        && Objects.equal(hardwareId, that.hardwareId)
        && Objects.equal(imageId, that.imageId)
        && awsEc2SpotPrice == that.awsEc2SpotPrice
        && Objects.equal(roles, that.roles);
    }
    return false;
  }

  public int hashCode() {
    return Objects.hashCode(numberOfInstances, minNumberOfInstances,
      hardwareId, imageId, awsEc2SpotPrice, roles);
  }

  public String toString() {
    return Objects.toStringHelper(this)
      .add("numberOfInstances", numberOfInstances)
      .add("minNumberOfInstances", minNumberOfInstances)
      .add("hardwareId", hardwareId)
      .add("imageId", imageId)
      .add("awsEc2SpotPrice", awsEc2SpotPrice)
      .add("roles", roles)
      .toString();
  }

  public static Map<String, String> parse(String... strings) {
    Set<String> roles = newLinkedHashSet(newArrayList(strings));
    Map<String, String> templates = Maps.newHashMap();
    for (String s : roles) {
      String[] parts = s.split(" ");
      checkArgument(parts.length == 2,
        "Invalid instance template syntax for '%s'. Does not match " +
          "'<number> <role1>+<role2>+<role3>...', e.g. '1 hadoop-namenode+hadoop-jobtracker'.", s);
      templates.put(parts[1], parts[0]);
    }
    return templates;
  }

  public static List<InstanceTemplate> parse(Configuration configuration)
      throws ConfigurationException {
    final String[] instanceTemplates = configuration.getStringArray(
      ClusterSpec.Property.INSTANCE_TEMPLATES.getConfigName());

    List<InstanceTemplate> templates = newArrayList();
    for (String s : instanceTemplates) {
      String[] parts = s.split(" ");

      checkArgument(parts.length == 2, "Invalid instance template syntax for '%s'. Does not match " +
        "'<number> <role1>+<role2>+<role3>...', e.g. '1 hadoop-namenode+hadoop-jobtracker'.", s);

      int numberOfInstances = Integer.parseInt(parts[0]);
      String templateGroup = parts[1];

      InstanceTemplate.Builder templateBuilder = InstanceTemplate.builder()
        .numberOfInstance(numberOfInstances)
        .roles(templateGroup.split("\\+"))
        .minNumberOfInstances(
          parseMinNumberOfInstances(configuration, templateGroup, numberOfInstances)
        );
      parseInstanceTemplateGroupOverrides(configuration, templateGroup, templateBuilder);

      templates.add(templateBuilder.build());
    }
    validateThatWeHaveNoOtherOverrides(templates, configuration);
    return templates;
  }

  private static void parseInstanceTemplateGroupOverrides(
    Configuration configuration, String templateGroup, Builder templateBuilder
  ) {

    String hardwareId = configuration.getString(
      "whirr.templates." + templateGroup + ".hardware-id", null);
    String imageId = configuration.getString(
      "whirr.templates." + templateGroup + ".image-id", null);
    float awsEc2SpotPrice = configuration.getFloat(
      "whirr.templates." + templateGroup + ".aws-ec2-spot-price", 0);

    templateBuilder.hardwareId(hardwareId).imageId(imageId)
      .awsEc2SpotPrice(awsEc2SpotPrice);
  }

  private static int parseMinNumberOfInstances(
    Configuration configuration, String templateGroup, int numberOfInstances
  ) {

    Map<String, String> maxPercentFailures = parse(configuration.getStringArray(
      ClusterSpec.Property.INSTANCE_TEMPLATES_MAX_PERCENT_FAILURES.getConfigName()));

    Map<String, String> minInstances = parse(configuration.getStringArray(
      ClusterSpec.Property.INSTANCE_TEMPLATES_MINIMUM_NUMBER_OF_INSTANCES.getConfigName()));

    int minNumberOfInstances = 0;
    String maxPercentFail = maxPercentFailures.get(templateGroup);

    if (maxPercentFail != null) {
      // round up integer division (a + b -1) / b
      minNumberOfInstances = (Integer.parseInt(maxPercentFail) * numberOfInstances + 99) / 100;
    }

    String minNumberOfInst = minInstances.get(templateGroup);
    if (minNumberOfInst != null) {
      int minExplicitlySet = Integer.parseInt(minNumberOfInst);
      if (minNumberOfInstances > 0) { // maximum between two minims
        minNumberOfInstances = Math.max(minNumberOfInstances, minExplicitlySet);
      } else {
        minNumberOfInstances = minExplicitlySet;
      }
    }

    if (minNumberOfInstances == 0 || minNumberOfInstances > numberOfInstances) {
      minNumberOfInstances = numberOfInstances;
    }

    return minNumberOfInstances;
  }

  private static void validateThatWeHaveNoOtherOverrides(
    List<InstanceTemplate> templates, Configuration configuration
  ) throws ConfigurationException {

    Set<String> groups = Sets.newHashSet(Iterables.transform(templates,
      new Function<InstanceTemplate, String>() {
        private final Joiner plusJoiner = Joiner.on("+");

        @Override
        public String apply(InstanceTemplate instance) {
          return plusJoiner.join(instance.getRoles());
        }
      }));

    Pattern pattern = Pattern.compile("^whirr\\.templates\\.([^.]+)\\..*$");
    Iterator iterator = configuration.getKeys("whirr.templates");

    while (iterator.hasNext()) {
      String key = String.class.cast(iterator.next());
      Matcher matcher = pattern.matcher(key);

      if (matcher.find() && !groups.contains(matcher.group(1))) {
        throw new ConfigurationException(String.format("'%s' is referencing a " +
          "template group not present in 'whirr.instance-templates'", key));
      }
    }
  }
}
