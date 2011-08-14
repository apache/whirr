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

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.whirr.ClusterSpec;
import org.jclouds.blobstore.AsyncBlobStore;
import org.jclouds.blobstore.BlobMap;
import org.jclouds.blobstore.BlobRequestSigner;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.BlobStoreContextFactory;
import org.jclouds.blobstore.InputStreamMap;
import org.jclouds.blobstore.attr.ConsistencyModel;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;
import org.jclouds.rest.RestContext;
import org.jclouds.rest.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.inject.AbstractModule;

public class BlobStoreContextBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(Cache.class);
   
  private static enum Cache implements Function<ClusterSpec, BlobStoreContext> {

    INSTANCE;

    @Override
    public BlobStoreContext apply(ClusterSpec arg0) {
      return cache.get(new Key(arg0));
    }

    // this should prevent recreating the same compute context twice
    @VisibleForTesting
    final Map<Key, BlobStoreContext> cache = new MapMaker().makeComputingMap(
       new Function<Key, BlobStoreContext>(){
        private final BlobStoreContextFactory factory =  new BlobStoreContextFactory();
        
        private final Set<AbstractModule> wiring = ImmutableSet.of(
            new SLF4JLoggingModule(), 
            new EnterpriseConfigurationModule());        
        
        @Override
        public BlobStoreContext apply(Key arg0) {
          LOG.debug("creating new BlobStoreContext {}", arg0);
          BlobStoreContext context = new IgnoreCloseBlobStoreContext(
              factory.createContext(arg0.provider, arg0.identity, arg0.credential,
                                    wiring, arg0.overrides));
          LOG.info("created new BlobStoreContext {}", context);
          return context;
       }
      
      }
    );
  }
  
  public static BlobStoreContext build(ClusterSpec spec) {
    return Cache.INSTANCE.apply(spec);
  }


  private static class IgnoreCloseBlobStoreContext extends ForwardingObject implements BlobStoreContext {

    private final BlobStoreContext context;

    public IgnoreCloseBlobStoreContext(final BlobStoreContext context) {
      this.context = context;
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          LOG.debug("closing BlobStoreContext {}", context);
          context.close();
        }
      });
    }

    @Override
    protected BlobStoreContext delegate() {
      return context;
    }

    @Override
    public BlobRequestSigner getSigner() {
      return delegate().getSigner();
    }

    @Override
    public InputStreamMap createInputStreamMap(String container, ListContainerOptions options) {
      return delegate().createInputStreamMap(container, options);
    }

    @Override
    public InputStreamMap createInputStreamMap(String container) {
      return delegate().createInputStreamMap(container);
    }

    @Override
    public BlobMap createBlobMap(String container, ListContainerOptions options) {
      return delegate().createBlobMap(container, options);
    }

    @Override
    public BlobMap createBlobMap(String container) {
      return delegate().createBlobMap(container);
    }

    @Override
    public AsyncBlobStore getAsyncBlobStore() {
      return delegate().getAsyncBlobStore();
    }

    @Override
    public BlobStore getBlobStore() {
      return delegate().getBlobStore();
    }

    @Override
    public ConsistencyModel getConsistencyModel() {
      return delegate().getConsistencyModel();
    }

    @Override
    public <S, A> RestContext<S, A> getProviderSpecificContext() {
      return delegate().getProviderSpecificContext();
    }

    @Override
    public Utils getUtils() {
      return delegate().getUtils();
    }

    @Override
    public Utils utils() {
      return delegate().utils();
    }

    @Override
    public void close() {
      // ignore; closed by shutdown hook
    }

  }

  /**
   * All APIs that are independently configurable.
   * @see <a href="http://code.google.com/p/jclouds/issues/detail?id=657" />
   */
  public static final Iterable<String> BLOBSTORE_APIS = ImmutableSet.of("transient", "file", "swift", "walrus",
      "atmos", "aws-s3", "cloudfiles-us", "cloudfiles-uk");

  /**
   *  jclouds providers and apis that can be used in BlobStoreContextFactory
   */
  public static final Iterable<String> BLOBSTORE_KEYS = Iterables.concat(
      Iterables.transform(Providers.allCompute(), new Function<ProviderMetadata, String>() {

        @Override
        public String apply(ProviderMetadata input) {
          return input.getId();
        }

      }), BLOBSTORE_APIS);

  /**
   * configurable properties, scoped to a provider.
   */
  public static final Iterable<String> PROVIDER_PROPERTIES = ImmutableSet.of("endpoint", "api", "apiversion",
      "iso3166-codes");

  /**
   * Key class for the blobstore context cache
   */
  private static class Key {
    private final String provider;
    private final String identity;
    private final String credential;
    private final String key;
    private final Properties overrides;

    public Key(ClusterSpec spec) {
      provider = spec.getBlobStoreProvider();
      identity = spec.getBlobStoreIdentity();
      credential = spec.getBlobStoreCredential();
      key = String.format("%s-%s-%s", provider, identity, credential);
      Configuration jcloudsConfig = spec.getConfigurationForKeysWithPrefix("jclouds");
      
      // jclouds configuration for providers are not prefixed with jclouds.
      for (String key : BLOBSTORE_KEYS) {
        for (String property : PROVIDER_PROPERTIES) {
          String prefixedProperty = "jclouds." + key + "." + property;
          if (jcloudsConfig.containsKey(prefixedProperty))
            jcloudsConfig.setProperty(key + "." + property, 
                jcloudsConfig.getProperty(prefixedProperty));
        }
      }

      overrides = ConfigurationConverter.getProperties(jcloudsConfig);
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof Key) {
        return Objects.equal(this.key, ((Key)that).key)
          && Objects.equal(overrides, ((Key)that).overrides);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(key, overrides);
    }
  }
}
