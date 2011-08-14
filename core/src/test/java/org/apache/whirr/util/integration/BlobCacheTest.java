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

package org.apache.whirr.util.integration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.BlobStoreContextBuilder;
import org.apache.whirr.service.ComputeCache;
import org.apache.whirr.util.BlobCache;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.http.HttpRequest;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class BlobCacheTest {

  private static final Logger LOG = LoggerFactory.getLogger(BlobCacheTest.class);

  private Configuration getTestConfiguration() throws ConfigurationException {
    return new PropertiesConfiguration("whirr-core-test.properties");
  }

  private ClusterSpec getTestClusterSpec() throws Exception {
    return ClusterSpec.withTemporaryKeys(getTestConfiguration());
  }

  @Test
  public void testUploadSmallFileToBlobCache() throws Exception {
    testBlobCacheUpload("dummy small content");
  }

  @Test
  public void testUploadLargeFileToBlobCache() throws Exception {
    testBlobCacheUpload(
        RandomStringUtils.randomAlphanumeric(1024 * 1024)
    );
  }

  @Test
  public void testUploadInEUS3Region() throws Exception {
    ClusterSpec spec = getTestClusterSpec();
    if ("aws-ec2".equals(spec.getProvider())) {

      // Configuration workaround need until the following issue is fixed
      // http://code.google.com/p/jclouds/issues/detail?id=656

      spec.setBlobStoreLocationId("EU");
      spec.getConfiguration().setProperty(
          "jclouds.aws-s3.endpoint", "https://s3-eu-west-1.amazonaws.com");

      testBlobCacheUpload(
          RandomStringUtils.randomAlphanumeric(1024 * 1024),
          spec
      );
    }
  }

  @Test
  public void testSelectBestLocation() throws Exception {
    ClusterSpec spec = getTestClusterSpec();
    if (!spec.getProvider().equals("aws") && !spec.getProvider().equals("aws-ec2")) {
      return; // this test can be executed only on amazon but the internal
      // location selection mechanism should work for any cloud provider
    }
    spec.setLocationId("eu-west-1");

    BlobCache cache = BlobCache.getInstance(ComputeCache.INSTANCE, spec);
    assertThat(cache.getLocation().getId(), is("EU"));
  }

  @Test
  public void testSpecifyCacheContainerInConfig() throws Exception {
    ClusterSpec spec = getTestClusterSpec();

    BlobStoreContext context = BlobStoreContextBuilder.build(spec);
    String container = generateRandomContainerName(context);
    LOG.info("Created temporary container '{}'", container);

    try {
      spec.setBlobStoreCacheContainer(container);

      BlobCache cache = BlobCache.getInstance(ComputeCache.INSTANCE, spec);
      assertThat(cache.getContainer(), is(container));
      cache.dropAndClose();

      assertThat(context.getBlobStore().containerExists(container), is(true));

    } finally {
      LOG.info("Removing temporary container '{}'", container);
      context.getBlobStore().deleteContainer(container);
    }
  }

  private void testBlobCacheUpload(String payload) throws Exception {
    testBlobCacheUpload(payload, getTestClusterSpec());
  }

  private void testBlobCacheUpload(String payload, ClusterSpec spec)
      throws Exception {
    File tempFile = createTemporaryFile(payload);

    BlobCache cache = BlobCache.getInstance(ComputeCache.INSTANCE, spec);

    try {
      cache.putIfAbsent(tempFile);

      HttpRequest req = cache.getSignedRequest(tempFile.getName());
      assertThat(readContent(req), is(payload));

      /* render download statement for visual test inspection */
      LOG.info(cache.getAsSaveToStatement("/tmp",
          tempFile.getName()).render(OsFamily.UNIX));

    } finally {
      BlobCache.dropAndCloseAll();
    }
  }

  private String generateRandomContainerName(BlobStoreContext context) {
    String candidate;
    do {
      candidate = RandomStringUtils.randomAlphanumeric(12).toLowerCase();
    } while(!context.getBlobStore().createContainerInLocation(null, candidate));
    return candidate;
  }

  private String readContent(HttpRequest req) throws IOException {
    HttpClient client = new DefaultHttpClient();
    try {
      HttpGet get = new HttpGet(req.getEndpoint());

      Map<String, Collection<String>> headers = req.getHeaders().asMap();
      for(String key : headers.keySet()) {
        for(String value : headers.get(key)) {
          get.addHeader(key, value);
        }
      }

      ResponseHandler<String> handler = new BasicResponseHandler();
      return client.execute(get, handler);

    } finally {
      client.getConnectionManager().shutdown();
    }
  }

  private File createTemporaryFile(String content) throws IOException {
    File tempFile = File.createTempFile("whirr", ".txt");
    tempFile.deleteOnExit();
    Files.write(content, tempFile, Charset.defaultCharset());
    return tempFile;
  }

}
