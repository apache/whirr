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

import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

public class Cluster {
  
  public static class Instance {
    private Set<String> roles;
    private InetAddress publicAddress;
    private InetAddress privateAddress;

    public Instance(Set<String> roles, InetAddress publicAddress,
	InetAddress privateAddress) {
      this.roles = roles;
      this.publicAddress = publicAddress;
      this.privateAddress = privateAddress;
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

}
