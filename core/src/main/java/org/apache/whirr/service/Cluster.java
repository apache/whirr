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
import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

import org.jclouds.domain.Credentials;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * This class represents a real cluster of {@link Instance}s.
 *
 */
public class Cluster {
  
  /**
   * This class represents a real node running in a cluster. An instance has
   * one or more roles.
   * @see org.apache.whirr.service.ClusterSpec.InstanceTemplate
   */
  public static class Instance {
    private final Credentials loginCredentials;
    private final Set<String> roles;
    private final InetAddress publicAddress;
    private final InetAddress privateAddress;
    private final String id;

    public Instance(Credentials loginCredentials, Set<String> roles, InetAddress publicAddress,
        InetAddress privateAddress, String id) {
      this.loginCredentials = checkNotNull(loginCredentials, "loginCredentials");
      this.roles = checkNotNull(roles, "roles");
      this.publicAddress = checkNotNull(publicAddress, "publicAddress");
      this.privateAddress = checkNotNull(privateAddress, "privateAddress");
      this.id = checkNotNull(id, "id");
    }

    public Credentials getLoginCredentials() {
      return loginCredentials;
    }
    
    public Set<String> getRoles() {
      return roles;
    }

    public InetAddress getPublicAddress() {
      return publicAddress;
    }

    public InetAddress getPrivateAddress() {
      return privateAddress;
    }
    
    public String getId() {
      return id;
    }
    
    public String toString() {
      return Objects.toStringHelper(this)
        .add("roles", roles)
        .add("publicAddress", publicAddress)
        .add("privateAddress", privateAddress)
        .add("id", id)
        .toString();
    }
    
  }
  
  private Set<Instance> instances;
  private Properties configuration;

  public Cluster(Set<Instance> instances) {
    this(instances, new Properties());
  }

  public Cluster(Set<Instance> instances, Properties configuration) {
    this.instances = instances;
    this.configuration = configuration;
  }
  
  public Set<Instance> getInstances() {
    return instances;
  }  
  public Properties getConfiguration() {
    return configuration;
  }

  public Instance getInstanceMatching(Predicate<Instance> predicate) {
    return Iterables.getOnlyElement(Sets.filter(instances, predicate));
  }

  public Set<Instance> getInstancesMatching(Predicate<Instance> predicate) {
    return Sets.filter(instances, predicate);
  }

  public String toString() {
    return Objects.toStringHelper(this)
      .add("instances", instances)
      .add("configuration", configuration)
      .toString();
  }
  
}
