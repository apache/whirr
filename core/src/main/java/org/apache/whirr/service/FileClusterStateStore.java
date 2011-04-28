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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.Set;

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.util.DnsUtil;
import org.jclouds.domain.Credentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Stores/Reads cluster state from a local file (located at:
 * "~/.whirr/cluster-name/instances")
 * 
 */
public class FileClusterStateStore extends ClusterStateStore {

  private static final Logger LOG = LoggerFactory
      .getLogger(FileClusterStateStore.class);

  private ClusterSpec spec;
  private Credentials credentials;

  public FileClusterStateStore(ClusterSpec spec) {
    checkNotNull(spec,"clusterSpec");

    this.spec = spec;
    this.credentials = new Credentials(spec.getClusterUser(), spec.getPrivateKey());
  }

  @Override
  public Cluster load() throws IOException {
    File instancesFile = new File(spec.getClusterDirectory(), "instances");
    Set<Cluster.Instance> instances = Files.readLines(instancesFile, Charsets.UTF_8,
        new LineProcessor<Set<Cluster.Instance>>() {

          Set<Cluster.Instance> instances = Sets.newLinkedHashSet();

          @Override
          public boolean processLine(String line) throws IOException {
            Iterator<String> fields = Splitter.on("\t").split(line).iterator();
            String id = fields.next();
            Set<String> roles = Sets.newLinkedHashSet(Splitter.on(",").split(
                fields.next()));
            String publicAddress = fields.next();
            String privateAddress = fields.next();
            instances.add(new Cluster.Instance(credentials, roles,
              InetAddress.getByName(publicAddress).getHostAddress(),
              privateAddress, id, null));
            return true;
          }

          @Override
          public Set<Cluster.Instance> getResult() {
            return instances;
          }
        });
    return new Cluster(instances);
  }

  @Override
  public void destroy() throws IOException {
    Files.deleteRecursively(spec.getClusterDirectory());
  }

  @Override
  public void save(Cluster cluster) throws IOException {
    File clusterDir = spec.getClusterDirectory();
    File instancesFile = new File(clusterDir, "instances");

    StringBuilder sb = new StringBuilder();
    for (Cluster.Instance instance : cluster.getInstances()) {
      String id = instance.getId();
      String roles = Joiner.on(',').join(instance.getRoles());

      String publicAddress = DnsUtil.resolveAddress(instance.getPublicAddress()
        .getHostAddress());
      String privateAddress = instance.getPrivateAddress().getHostAddress();

      sb.append(id).append("\t");
      sb.append(roles).append("\t");
      sb.append(publicAddress).append("\t");
      sb.append(privateAddress).append("\n");
    }

    try {
      Files.write(sb.toString(), instancesFile, Charsets.UTF_8);
      LOG.info("Wrote instances file {}", instancesFile);
    } catch (IOException e) {
      LOG.error("Problem writing instances file {}", instancesFile, e);
    }
  }

}
