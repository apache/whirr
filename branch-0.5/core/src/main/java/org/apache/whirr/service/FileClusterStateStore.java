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

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.io.Files;

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

  public FileClusterStateStore(ClusterSpec spec) {
    checkNotNull(spec,"clusterSpec");
    this.spec = spec;
  }

  @Override
  public Cluster load() throws IOException {
    File instancesFile = new File(spec.getClusterDirectory(), "instances");
    return unserialize(spec,
      Joiner.on("\n").join(Files.readLines(instancesFile, Charsets.UTF_8)));
  }

  @Override
  public void destroy() throws IOException {
    Files.deleteRecursively(spec.getClusterDirectory());
  }

  @Override
  public void save(Cluster cluster) throws IOException {
    File instancesFile = new File(spec.getClusterDirectory(), "instances");

    try {
      Files.write(serialize(cluster).toString(), instancesFile, Charsets.UTF_8);
      LOG.info("Wrote instances file {}", instancesFile);

    } catch (IOException e) {
      LOG.error("Problem writing instances file {}", instancesFile, e);
    }
  }

}
