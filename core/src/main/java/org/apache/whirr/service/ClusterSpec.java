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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ClusterSpec {
  
  public static class InstanceTemplate {
    private Set<String> roles;
    private int numberOfInstances;

    public InstanceTemplate(int numberOfInstances, String... roles) {
      this(numberOfInstances, Sets.newHashSet(roles));
    }

    public InstanceTemplate(int numberOfInstances, Set<String> roles) {
      this.numberOfInstances = numberOfInstances;
      this.roles = roles;
    }

    public Set<String> getRoles() {
      return roles;
    }

    public int getNumberOfInstances() {
      return numberOfInstances;
    }
    
  }
  
  private Properties configuration;
  private List<InstanceTemplate> instanceTemplates;
  private Map<Set<String>, InstanceTemplate> instanceTemplatesMap = Maps.newHashMap();
  
  public ClusterSpec(InstanceTemplate... instanceTemplates) {
    this(Arrays.asList(instanceTemplates));
  }

  public ClusterSpec(List<InstanceTemplate> instanceTemplates) {
    this(new Properties(), instanceTemplates);
  }

  public ClusterSpec(Properties configuration, List<InstanceTemplate> instanceTemplates) {
    this.configuration = configuration;
    this.instanceTemplates = instanceTemplates;
    for (InstanceTemplate template : instanceTemplates) {
      instanceTemplatesMap.put(template.roles, template);
    }
  }

  public Properties getConfiguration() {
    return configuration;
  }

  public List<InstanceTemplate> getInstanceTemplates() {
    return instanceTemplates;
  }
  
  public InstanceTemplate getInstanceTemplate(Set<String> roles) {
    return instanceTemplatesMap.get(roles);
  }
  
  public InstanceTemplate getInstanceTemplate(String... roles) {
    return getInstanceTemplate(Sets.newHashSet(roles));
  }
  
}
