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

import org.apache.commons.io.IOUtils;
import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterSpec;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class BlobClusterStateStore extends ClusterStateStore {

  private static final Logger LOG = LoggerFactory
    .getLogger(FileClusterStateStore.class);

  private ClusterSpec spec;
  private BlobStoreContext context;

  private String container;
  private String blobName;

  public BlobClusterStateStore(ClusterSpec spec) {
    this.spec = spec;
    this.context = BlobStoreContextBuilder.build(spec);

    this.container = checkNotNull(spec.getStateStoreContainer());
    this.blobName = checkNotNull(spec.getStateStoreBlob());

    /* create container if it does not already exists */
    if (!context.getBlobStore().containerExists(container)) {
      context.getBlobStore().createContainerInLocation(null, container);
    }
  }

  @Override
  public Cluster load() throws IOException {
    Blob blob = context.getBlobStore().getBlob(container, blobName);
    if (blob != null) {
      return unserialize(spec,
        IOUtils.toString(blob.getPayload().getInput(), "utf-8"));
    }
    return null;
  }

  @Override
  public void save(Cluster cluster) throws IOException {
    BlobStore store = context.getBlobStore();

    Blob blob = store.newBlob(blobName);
    blob.setPayload(serialize(cluster));
    store.putBlob(container, blob);

    LOG.info("Saved cluster state to '{}' ", context.getSigner()
      .signGetBlob(container, blobName).getEndpoint().toString());
  }

  @Override
  public void destroy() throws IOException {
    context.getBlobStore().removeBlob(container, blobName);
  }
}
