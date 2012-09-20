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
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Properties;
import java.util.Set;

import org.apache.whirr.net.DnsResolver;
import org.apache.whirr.net.FastDnsResolver;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.net.InetAddresses;

/**
 * This class represents a real cluster of {@link Instance}s.
 *
 */
public class Cluster {

  private static final Logger LOG = LoggerFactory.getLogger(Cluster.class);

  /**
   * This class represents a real node running in a cluster. An instance has
   * one or more roles.
   * @see InstanceTemplate
   */
  public static class Instance {

    private final Credentials loginCredentials;
    private final Set<String> roles;
    private final String publicIp;
    private String publicHostName;
    private final String privateIp;
    private String privateHostName;
    private final String id;
    private final NodeMetadata nodeMetadata;
    private final DnsResolver dnsResolver;

    public Instance(Credentials loginCredentials, Set<String> roles, String publicIp,
        String privateIp, String id, NodeMetadata nodeMetadata) {
      this(loginCredentials, roles, publicIp, privateIp, id, nodeMetadata, new FastDnsResolver());
    }

    public Instance(Credentials loginCredentials, Set<String> roles, String publicIp,
        String privateIp, String id, NodeMetadata nodeMetadata, DnsResolver dnsResolver) {
      this.loginCredentials = checkNotNull(loginCredentials, "loginCredentials");
      this.roles = checkNotNull(roles, "roles");
      this.publicIp = checkNotNull(publicIp, "publicIp");
      checkArgument(InetAddresses.isInetAddress(publicIp),
          "invalid IP address: %s", publicIp);
      this.privateIp = privateIp;
      if (privateIp != null) {
        checkArgument(InetAddresses.isInetAddress(privateIp),
            "invalid IP address: %s", privateIp);
      }
      this.id = checkNotNull(id, "id");
      this.nodeMetadata = nodeMetadata;
      this.dnsResolver = dnsResolver;

      LOG.debug("constructed instance {} with IP public {}, private {}, and DNS resolver {}",
              new Object[] { this, publicIp, privateIp, dnsResolver });
    }

    public Credentials getLoginCredentials() {
      return loginCredentials;
    }

    public Set<String> getRoles() {
      return roles;
    }

    public InetAddress getPublicAddress() throws IOException {
      return resolveIpAddress(getPublicIp(), getPublicHostName());
    }

    public InetAddress getPrivateAddress() throws IOException {
      return resolveIpAddress(getPrivateIp(), getPrivateHostName());
    }

    private InetAddress resolveIpAddress(String ip, String host) throws IOException {
      byte[] addr = InetAddresses.forString(ip).getAddress();
      return InetAddress.getByAddress(host, addr);
    }

    public String getPublicIp() {
      return publicIp;
    }

    public synchronized String getPublicHostName() throws IOException {
      if (publicHostName == null) {
        LOG.debug("resolving public hostname of {} (public {}, private {})", new Object[] { this, publicIp, privateIp });
        publicHostName = dnsResolver.apply(publicIp);
        LOG.debug("resolved public hostname of {} as {}", this, publicHostName);
        if (publicHostName.matches("[0-9\\.]+") && nodeMetadata.getHostname()!=null && !nodeMetadata.getHostname().isEmpty()) {
            LOG.debug("overriding public hostname of {} from {} (unresolved) to {}", new Object[] { this, publicHostName, nodeMetadata.getHostname() });
            publicHostName = nodeMetadata.getHostname();
        }
      }
      return publicHostName;
    }

    public String getPrivateIp() {
      return privateIp;
    }

    /**
     * TODO Remove in version 0.9.0
     */
    @Deprecated
    public synchronized String getPrivateHostName() throws IOException {
      return privateIp;
    }

    public String getId() {
      return id;
    }

    public NodeMetadata getNodeMetadata() {
      return nodeMetadata;
    }

    public String toString() {
      return Objects.toStringHelper(this)
        .add("roles", roles)
        .add("publicIp", publicIp)
        .add("privateIp", privateIp)
        .add("id", id)
        .add("nodeMetadata", nodeMetadata)
        .toString();
    }
  }

  public static Cluster empty() {
    return new Cluster(Sets.<Instance>newHashSet());
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

  public void removeInstancesMatching(Predicate<Instance> predicate) {
    instances = Sets.filter(instances, Predicates.not(predicate));
  }

  public String toString() {
    return Objects.toStringHelper(this)
      .add("instances", instances)
      .toString();
  }

}
