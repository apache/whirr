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

package org.apache.whirr.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.BlobStoreContextBuilder;
import org.apache.whirr.service.jclouds.SaveHttpResponseTo;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.domain.Location;
import org.jclouds.http.HttpRequest;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import static org.jclouds.blobstore.options.PutOptions.Builder.multipart;

public class BlobCache {

  private static final Logger LOG = LoggerFactory.getLogger(BlobCache.class);

  private static Map<ClusterSpec, BlobCache> instances = Maps.newHashMap();

  static {
    /* Ensure that all created containers are removed when the JVM stops */
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        BlobCache.dropAndCloseAll();
      }
    });
  }

  public synchronized static BlobCache getInstance(Function<ClusterSpec, ComputeServiceContext> getCompute,
      ClusterSpec spec) throws IOException {
    if (!instances.containsKey(spec)) {
      try {
        instances.put(spec.copy(), new BlobCache(getCompute, spec));
      } catch (ConfigurationException e) {
        throw new IOException(e);
      }
    }
    return instances.get(spec);
  }

  public synchronized static void dropAndCloseAll() {
    for(BlobCache instance : instances.values()) {
      instance.dropAndClose();
    }
    instances.clear();
  }

  final BlobStoreContext context;
  final Function<ClusterSpec, ComputeServiceContext> getCompute;

  String container = null;
  boolean temporary = true;

  Location defaultLocation = null;

  private BlobCache(Function<ClusterSpec, ComputeServiceContext> getCompute,
      ClusterSpec spec) throws IOException {
    this.getCompute = getCompute;
    this.context = BlobStoreContextBuilder.build(spec);

    if (spec.getBlobStoreCacheContainer() != null) {
      this.container = spec.getBlobStoreCacheContainer();
      this.temporary = false;
    }

    updateDefaultLocation(spec);
  }

  public Location getLocation() {
    return defaultLocation;
  }

  private void updateDefaultLocation(ClusterSpec spec) throws IOException {
    if (spec.getBlobStoreLocationId() != null) {
      /* find the location with the given Id */
      for(Location loc : context.getBlobStore().listAssignableLocations()) {
        if (loc.getId().equals(spec.getBlobStoreLocationId())) {
          defaultLocation = loc;
          break;
        }
      }
      if (defaultLocation == null) {
        LOG.warn("No blob store location found with this ID '{}'. " +
          "Using default location.", spec.getBlobStoreLocationId());
      }
    } else if (spec.getLocationId() != null) {
      /* find the closest location to the compute nodes */
      ComputeServiceContext compute = getCompute.apply(spec);

      Set<String> computeIsoCodes = null;
      for(Location loc : compute.getComputeService().listAssignableLocations()) {
        if (loc.getId().equals(spec.getLocationId())) {
          computeIsoCodes = loc.getIso3166Codes();
          break;
        }
      }
      if (computeIsoCodes == null) {
        LOG.warn("Invalid compute location ID '{}'. " +
          "Using default blob store location.", spec.getLocationId());
      } else {
        for (Location loc : context.getBlobStore().listAssignableLocations()) {
          if (containsAny(loc.getIso3166Codes(), computeIsoCodes)) {
            defaultLocation = loc;
            break;
          }
        }
      }
    }
  }

  private <T> boolean containsAny(Set<T> set1, Set<T> set2) {
    for (T el : set1) {
      if (set2.contains(el)) {
        return true;
      }
    }
    return false;
  }

  public synchronized void putIfAbsent(String localUri) throws URISyntaxException, IOException {
    putIfAbsent(new URI(localUri));
  }

  public synchronized void putIfAbsent(URI uri) throws IOException {
    try {
      putIfAbsent(new File(uri));
    } catch(FileNotFoundException e) {
      throw new IOException(e);
    }
  }

  public synchronized void putIfAbsent(File file) throws FileNotFoundException {
      allocateContainer();

      BlobStore store = context.getBlobStore();
      if (!store.blobExists(container, file.getName())) {
        LOG.info("Uploading '{}' to '{}' blob cache.", file.getName(), container);

        Blob blob = context.getBlobStore().blobBuilder(container)
                .name(file.getName())
                .payload(file)
                .contentLength(file.length())
                .build();

        store.putBlob(container, blob, multipart());
      }
  }

  public synchronized Statement getAsSaveToStatement(String target, String name) throws IOException {
    HttpRequest req = getSignedRequest(name);
    return new SaveHttpResponseTo(target, name, req.getMethod(), req.getEndpoint(), req.getHeaders());
  }

  public synchronized Statement getAsSaveToStatement(String target, URI uri) throws IOException {
    return getAsSaveToStatement(target, new File(uri).getName());
  }

  public synchronized HttpRequest getSignedRequest(String blobName) throws IOException {
    checkExistsBlob(blobName);
    return context.getSigner().signGetBlob(container, blobName);
  }

  public String getContainer() {
    return container;
  }

  private void checkExistsBlob(String name) throws IOException {
    if (container == null || !context.getBlobStore().blobExists(container, name)) {
      throw new IOException("Blob not found: " + container + ":" + name);
    }
  }

  private void allocateContainer() {
    if (container == null) {
      container = generateRandomContainerName();
    }
  }

  private String generateRandomContainerName() {
    String candidate;
    do {
      candidate = RandomStringUtils.randomAlphanumeric(12).toLowerCase();
    } while(!context.getBlobStore().createContainerInLocation(defaultLocation, candidate));
    LOG.info("Created blob cache container '{}' located in '{}'", candidate, defaultLocation);
    return candidate;
  }

  public synchronized void dropAndClose() {
    if (container != null && temporary) {
      LOG.info("Removing blob cache '{}'", container);
      context.getBlobStore().deleteContainer(container);
    }
    context.close();
  }
}
