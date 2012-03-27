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

import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.lang.StringUtils;
import org.apache.whirr.ClusterSpec;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.ComputeServiceContextFactory;
import org.jclouds.compute.Utils;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.events.StatementOnNodeCompletion;
import org.jclouds.compute.events.StatementOnNodeFailure;
import org.jclouds.compute.events.StatementOnNodeSubmission;
import org.jclouds.domain.Credentials;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;
import org.jclouds.rest.RestContext;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.AllowConcurrentEvents;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Module;

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
  
  // this should prevent recreating the same compute context twice
  @VisibleForTesting
  final LoadingCache<Key, ComputeServiceContext> cache = CacheBuilder.newBuilder().build(
        new CacheLoader<Key, ComputeServiceContext>(){
        private final ComputeServiceContextFactory factory =  new ComputeServiceContextFactory();
               
        @Override
        public ComputeServiceContext load(Key arg0) {
          LOG.debug("creating new ComputeServiceContext {}", arg0);
          ComputeServiceContext context = new IgnoreCloseComputeServiceContext(factory.createContext(
            arg0.provider, arg0.identity, arg0.credential,
            ImmutableSet.<Module>of(), arg0.overrides));
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
    public <S, A> RestContext<S, A> getProviderSpecificContext() {
      return delegate().getProviderSpecificContext();
    }

    @Override
    public Map<String, Credentials> getCredentialStore() {
      return delegate().getCredentialStore();
    }

    @Override
    public Map<String, Credentials> credentialStore() {
      return delegate().credentialStore();
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
      /* Do nothing. The instance is closed by the builder */
    }

  }

  /**
   * All APIs that are independently configurable.
   * @see <a href="http://code.google.com/p/jclouds/issues/detail?id=657" />
   */
  public static final Iterable<String> COMPUTE_APIS = ImmutableSet.of(
      "stub", "nova", "vcloud", "elasticstack",
      "eucalyptus", "deltacloud", "byon"
  );

  /**
   *  jclouds providers and apis that can be used in ComputeServiceContextFactory
   */
  public static final Iterable<String> COMPUTE_KEYS = Iterables.concat(
      Iterables.transform(Providers.allCompute(), new Function<ProviderMetadata, String>() {

        @Override
        public String apply(ProviderMetadata input) {
          return input.getId();
        }

      }), COMPUTE_APIS);

  /**
   * configurable properties, scoped to a provider.
   */
  public static final Iterable<String> PROVIDER_PROPERTIES = ImmutableSet.of(
    "endpoint", "api", "apiversion", "iso3166-codes");

  /**
   * Key class for the compute context cache
   */
  private static class Key {
    private String provider;
    private String identity;
    private String credential;

    private final String key;
    private final Properties overrides;

    public Key(ClusterSpec spec) {
      provider = spec.getProvider();
      identity = spec.getIdentity();
      credential = spec.getCredential();

      key = String.format("%s-%s-%s", provider, identity, credential);
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
      if (spec.getBootstrapUser() != null) {
         overrides.put(provider + ".image.login-user", spec.getBootstrapUser());
      }

      if ("aws-ec2".equals(spec.getProvider()) && spec.getImageId() != null) {
        enableAWSEC2LazyImageFetching(spec);
      }

      if ("stub".equals(spec.getProvider())) {
        overrides.setProperty("jclouds.modules",
          SLF4JLoggingModule.class.getName() + ",org.apache.whirr.service.DryRunModule"
        );
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

      String[] parts = StringUtils.split(spec.getImageId(), '/');
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
        .toString();
    }
  }

}
