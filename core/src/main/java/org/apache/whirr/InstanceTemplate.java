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

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class describes the type of instances that should be in the cluster.
 * This is done by specifying the number of instances in each role.
 */
public class InstanceTemplate {
  private static Map<String, String> aliases = new HashMap<String, String>();
  private static final Logger LOG = LoggerFactory.getLogger(InstanceTemplate.class);

  static {
    /*
     * WARNING: this is not a generic aliasing mechanism. This code
     * should be removed in the following releases and it's
     * used only temporary to deprecate short legacy role names.
     */
    aliases.put("nn", "hadoop-namenode");
    aliases.put("jt", "hadoop-jobtracker");
    aliases.put("dn", "hadoop-datanode");
    aliases.put("tt", "hadoop-tasktracker");
    aliases.put("zk", "zookeeper");
  }

  private Set<String> roles;
  private int numberOfInstances;
  private int minNumberOfInstances;  // some instances may fail, at least a minimum number is required

  public InstanceTemplate(int numberOfInstances, String... roles) {
    this(numberOfInstances, numberOfInstances, Sets.newLinkedHashSet(Lists.newArrayList(roles)));
  }

  public InstanceTemplate(int numberOfInstances, Set<String> roles) {
    this(numberOfInstances, numberOfInstances, roles);
  }

  public InstanceTemplate(int numberOfInstances, int minNumberOfInstances, String... roles) {
    this(numberOfInstances, minNumberOfInstances, Sets.newLinkedHashSet(Lists.newArrayList(roles)));
  }

  public InstanceTemplate(int numberOfInstances, int minNumberOfInstances, Set<String> roles) {
    for (String role : roles) {
      checkArgument(!StringUtils.contains(role, " "),
          "Role '%s' may not contain space characters.", role);
    }

    this.roles = replaceAliases(roles);
    this.numberOfInstances = numberOfInstances;
    this.minNumberOfInstances = minNumberOfInstances;
  }

  private static Set<String> replaceAliases(Set<String> roles) {
    Set<String> newRoles = Sets.newLinkedHashSet();
    for(String role : roles) {
      if (aliases.containsKey(role)) {
        LOG.warn("Role name '{}' is deprecated, use '{}'",
            role, aliases.get(role));
        newRoles.add(aliases.get(role));
      } else {
        newRoles.add(role);
      }
    }
    return newRoles;
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

  public boolean equals(Object o) {
    if (o instanceof InstanceTemplate) {
      InstanceTemplate that = (InstanceTemplate) o;
      return Objects.equal(numberOfInstances, that.numberOfInstances)
        && Objects.equal(minNumberOfInstances, that.minNumberOfInstances)
        && Objects.equal(roles, that.roles);
    }
    return false;
  }

  public int hashCode() {
    return Objects.hashCode(numberOfInstances, minNumberOfInstances, roles);
  }

  public String toString() {
    return Objects.toStringHelper(this)
      .add("numberOfInstances", numberOfInstances)
      .add("minNumberOfInstances", minNumberOfInstances)
      .add("roles", roles)
      .toString();
  }

  public static Map<String, String> parse(String... strings) {
    Set<String> roles = Sets.newLinkedHashSet(Lists.newArrayList(strings));
    roles = replaceAliases(roles);
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

  public static List<InstanceTemplate> parse(Configuration cconf) {
    final String[] strings = cconf.getStringArray(ClusterSpec.Property.INSTANCE_TEMPLATES.getConfigName());
    Map<String, String> maxPercentFailures = parse(cconf.getStringArray(ClusterSpec.Property.INSTANCE_TEMPLATES_MAX_PERCENT_FAILURES.getConfigName()));
    Map<String, String> minInstances = parse(cconf.getStringArray(ClusterSpec.Property.INSTANCE_TEMPLATES_MINIMUM_NUMBER_OF_INSTANCES.getConfigName()));
    List<InstanceTemplate> templates = Lists.newArrayList();
    for (String s : strings) {
      String[] parts = s.split(" ");
      checkArgument(parts.length == 2,
          "Invalid instance template syntax for '%s'. Does not match " +
          "'<number> <role1>+<role2>+<role3>...', e.g. '1 hadoop-namenode+hadoop-jobtracker'.", s);
      int num = Integer.parseInt(parts[0]);
      int minNumberOfInstances = 0;
      final String maxPercentFail = maxPercentFailures.get(parts[1]);
      if (maxPercentFail != null) {
        // round up integer division (a + b -1) / b
        minNumberOfInstances = (Integer.parseInt(maxPercentFail) * num + 99) / 100;
      }
      String minNumberOfInst = minInstances.get(parts[1]);
      if (minNumberOfInst != null) {
        int minExplicitlySet = Integer.parseInt(minNumberOfInst);
        if (minNumberOfInstances > 0) { // maximum between two minims
          minNumberOfInstances = Math.max(minNumberOfInstances, minExplicitlySet);
        } else {
          minNumberOfInstances = minExplicitlySet;
        }
      }
      if (minNumberOfInstances == 0 || minNumberOfInstances > num) {
        minNumberOfInstances = num;
      }
      templates.add(new InstanceTemplate(num, minNumberOfInstances, parts[1].split("\\+")));
    }
    return templates;
  }
}
