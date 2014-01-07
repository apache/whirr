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

import static com.google.common.base.Preconditions.checkArgument;
import static org.jclouds.aws.ec2.reference.AWSEC2Constants.PROPERTY_EC2_AMI_QUERY;
import static org.jclouds.aws.ec2.reference.AWSEC2Constants.PROPERTY_EC2_CC_AMI_QUERY;
import static org.jclouds.location.reference.LocationConstants.PROPERTY_REGION;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.common.reflect.TypeToken;
import com.google.inject.Module;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.whirr.ClusterSpec;
import org.jclouds.Context;
import org.jclouds.ContextBuilder;
import org.jclouds.apis.ApiMetadata;
import org.jclouds.apis.Apis;
import org.jclouds.byon.BYONApiMetadata;
import org.jclouds.byon.Node;
import org.jclouds.byon.config.BYONComputeServiceContextModule;
import org.jclouds.byon.config.CacheNodeStoreModule;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.Utils;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.events.StatementOnNodeCompletion;
import org.jclouds.compute.events.StatementOnNodeFailure;
import org.jclouds.compute.events.StatementOnNodeSubmission;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A convenience class for building jclouds {@link ComputeServiceContext} objects.
 */
// singleton enum pattern
public enum ComputeCache implements Function<ClusterSpec, ComputeServiceContext> {
   
  INSTANCE;
  
  private static final Logger LOG = LoggerFactory.getLogger(ComputeCache.class);

  @Override
  public ComputeServiceContext apply(ClusterSpec arg0) {
    return cache.getUnchecked(new Key(arg0));
  }
  
  public void invalidate(ClusterSpec arg0) {
    cache.invalidate(new Key(arg0));
  }
  
  // this should prevent recreating the same compute context twice
  @VisibleForTesting
  final LoadingCache<Key, ComputeServiceContext> cache = CacheBuilder.newBuilder().build(
        new CacheLoader<Key, ComputeServiceContext>(){
               
        @Override
        public ComputeServiceContext load(Key arg0) {
          LOG.debug("creating new ComputeServiceContext {}", arg0);
          ContextBuilder builder;

          if (arg0.overrideApiMetadata != null) {
            builder = ContextBuilder.newBuilder(arg0.overrideApiMetadata)
              .credentials(arg0.identity, arg0.credential)
              .overrides(arg0.overrides)
              .modules(arg0.modules);
          } else {
            builder = ContextBuilder.newBuilder(arg0.provider)
            .credentials(arg0.identity, arg0.credential)
            .overrides(arg0.overrides)
            .modules(arg0.modules);
          }
          
          if (arg0.endpoint != null)
            builder.endpoint(arg0.endpoint);
          ComputeServiceContext context = new IgnoreCloseComputeServiceContext(
                builder.buildView(ComputeServiceContext.class));
          LOG.debug("created new ComputeServiceContext {}", context);
          context.utils().eventBus().register(ComputeCache.this);
          return context;
        }
    
    }
  );

  @Subscribe
  @AllowConcurrentEvents
  public void onStart(StatementOnNodeSubmission event) {
    LOG.info(">> running {} on node({})", event.getStatement(), event.getNode().getId());
    if (LOG.isDebugEnabled()) {
      LOG.debug(">> script for {} on node({})\n{}", new Object[] { event.getStatement(), event.getNode().getId(),
          event.getStatement().render(OsFamily.UNIX) });
    }
  }

  @Subscribe
  @AllowConcurrentEvents
  public void onFailure(StatementOnNodeFailure event) {
    LOG.error("<< error running {} on node({}): {}", new Object[] { event.getStatement(), event.getNode().getId(),
        event.getCause().getMessage() }, event.getCause());
  }

  @Subscribe
  @AllowConcurrentEvents
  public void onSuccess(StatementOnNodeCompletion event) {
    ExecResponse arg0 = event.getResponse();
    if (arg0.getExitStatus() != 0) {
      LOG.error("<< error running {} on node({}): {}", new Object[] { event.getStatement(), event.getNode().getId(),
          arg0 });
    } else {
      LOG.info("<< success executing {} on node({}): {}", new Object[] { event.getStatement(),
          event.getNode().getId(), arg0 });
    }

  }
  
  private static class IgnoreCloseComputeServiceContext
    extends ForwardingObject implements ComputeServiceContext {

    private final ComputeServiceContext context;

    public IgnoreCloseComputeServiceContext(final ComputeServiceContext context) {
      this.context = context;
      Runtime.getRuntime().addShutdownHook(new Thread() {
         @Override
         public void run() {
           LOG.debug("closing ComputeServiceContext {}", context);
           context.close();
         }
       });
    }

    @Override
    protected ComputeServiceContext delegate() {
       return context;
    }

    @Override
    public ComputeService getComputeService() {
      return delegate().getComputeService();
    }

    @Override
    public Utils utils() {
      return delegate().utils();
    }

    @Override
    public void close() {
       /* Do nothing. The instance is closed by the builder */
    }

    @Override
    public TypeToken<?> getBackendType() {
      return delegate().getBackendType();
    }

    @Override
    public <A extends Closeable> A unwrapApi(Class<A> apiClass) {
      return delegate().unwrapApi(apiClass);
    }

    @Override
    public <C extends Context> C unwrap(TypeToken<C> type) {
      return delegate().<C>unwrap(type);
    }

    @Override
    public <C extends Context> C unwrap() {
      return delegate().<C>unwrap();
    }

  }

  public static final Map<String, ApiMetadata> COMPUTE_APIS = Maps.uniqueIndex(Apis.viewableAs(ComputeServiceContext.class),
      Apis.idFunction());
  
  public static final Map<String, ProviderMetadata> COMPUTE_PROVIDERS = Maps.uniqueIndex(Providers.viewableAs(ComputeServiceContext.class),
      Providers.idFunction());
     
  public static final Set<String> COMPUTE_KEYS = ImmutableSet.copyOf(Iterables.concat(COMPUTE_PROVIDERS.keySet(), COMPUTE_APIS.keySet()));

  /**
   * configurable properties, scoped to a provider.
   */
  public static final Iterable<String> PROVIDER_PROPERTIES = ImmutableSet.of(
    "endpoint", "api", "apiversion", "iso3166-codes","nodes");

  /**
   * Key class for the compute context cache
   */
  private static class Key {
    private String provider;
    private String endpoint;
    private String identity;
    private String credential;
    private String clusterName;
    private ApiMetadata overrideApiMetadata;
    
    private final String key;
    private final Properties overrides;
    private final Set<Module> modules;


    public Key(ClusterSpec spec) {
      provider = spec.getProvider();
      endpoint = spec.getEndpoint();
      identity = spec.getIdentity();
      credential = spec.getCredential();
      clusterName = spec.getClusterName();

      key = Objects.toStringHelper("").omitNullValues()
            .add("provider", provider)
            .add("endpoint", endpoint)
            .add("identity", identity)
            .add("clusterName", clusterName).toString();
      Configuration jcloudsConfig = spec.getConfigurationForKeysWithPrefix("jclouds");
      
      // jclouds configuration for providers are not prefixed with jclouds.
      for (String key : COMPUTE_KEYS) {
        for (String property : PROVIDER_PROPERTIES) {
          String prefixedProperty = "jclouds." + key + "." + property;

          if (jcloudsConfig.containsKey(prefixedProperty)) {
            jcloudsConfig.setProperty(key + "." + property, 
                jcloudsConfig.getProperty(prefixedProperty));
          }
        }
      }
      overrides = ConfigurationConverter.getProperties(jcloudsConfig);

      if ("aws-ec2".equals(spec.getProvider()) && spec.getTemplate().getImageId() != null) {
        enableAWSEC2LazyImageFetching(spec);
      }

      if ("stub".equals(spec.getProvider())) {
        modules = ImmutableSet.<Module>of(new SLF4JLoggingModule(), new DryRunModule());
      } else if ("byon".equals(spec.getProvider()) && !spec.getByonNodes().isEmpty()) {
        overrideApiMetadata = new BYONApiMetadata()
          .toBuilder()
          .defaultModule(BYONComputeServiceContextModule.class)
          .build();

        modules = ImmutableSet.<Module>of(new SLF4JLoggingModule(), 
                                          new EnterpriseConfigurationModule(),
                                          new SshjSshClientModule(),
                                          new CacheNodeStoreModule(ImmutableMap.<String,Node>copyOf(spec.getByonNodes())));
      } else {
        modules = ImmutableSet.<Module>of(new SLF4JLoggingModule(), 
            new EnterpriseConfigurationModule(), new SshjSshClientModule());
      }
    }

    /**
     * AWS EC2 specific optimisation to avoid running queries across
     * all regiosn when the image-id is know from the property file
     *
     * @param spec
     */
    private void enableAWSEC2LazyImageFetching(ClusterSpec spec) {
      overrides.setProperty(PROPERTY_EC2_AMI_QUERY, "");
      overrides.setProperty(PROPERTY_EC2_CC_AMI_QUERY, "");

      String[] parts = StringUtils.split(spec.getTemplate().getImageId(), '/');
      checkArgument(parts.length == 2, "Expected to find image-id = region/ami-id");

      overrides.setProperty(PROPERTY_REGION, parts[0]);
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
    
    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("provider", provider)
        .add("identity", identity)
        .add("overrides", overrides)
        .add("clusterName", clusterName)
        .toString();
    }
  }

}
