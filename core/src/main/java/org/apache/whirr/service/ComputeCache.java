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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ForwardingObject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.inject.AbstractModule;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.jclouds.TakeLoginCredentialsFromWhirrProperties;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.ComputeServiceContextFactory;
import org.jclouds.compute.Utils;
import org.jclouds.domain.Credentials;
import org.jclouds.ec2.compute.strategy.EC2PopulateDefaultLoginCredentialsForImageStrategy;
import org.jclouds.enterprise.config.EnterpriseConfigurationModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.providers.ProviderMetadata;
import org.jclouds.providers.Providers;
import org.jclouds.rest.RestContext;
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
    return cache.get(new Key(arg0));
  }
  
  // this should prevent recreating the same compute context twice
  @VisibleForTesting
  final Map<Key, ComputeServiceContext> cache = new MapMaker().makeComputingMap(
      new Function<Key, ComputeServiceContext>(){
        private final ComputeServiceContextFactory factory =  new ComputeServiceContextFactory();
        private final Set<AbstractModule> wiring = ImmutableSet.of(
              new SshjSshClientModule(),
              new SLF4JLoggingModule(), 
              new EnterpriseConfigurationModule(),
              new BindLoginCredentialsPatchForEC2());        
        @Override
        public ComputeServiceContext apply(Key arg0) {
          LOG.debug("creating new ComputeServiceContext {}", arg0);
          ComputeServiceContext context = new IgnoreCloseComputeServiceContext(factory.createContext(
            arg0.provider, arg0.identity, arg0.credential,
            wiring, arg0.overrides));
          LOG.debug("created new ComputeServiceContext {}", context);
          return context;
        }
    
    }
  );
   
  private static class IgnoreCloseComputeServiceContext extends ForwardingObject implements ComputeServiceContext {

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
  public static final Iterable<String> COMPUTE_APIS = ImmutableSet.of("stub", "nova", "vcloud", "elasticstack",
      "eucalyptus", "deltacloud", "byon");

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
  public static final Iterable<String> PROVIDER_PROPERTIES = ImmutableSet.of("endpoint", "api", "apiversion",
      "iso3166-codes");

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
  
  //patch until jclouds 1.0-beta-10
  private static class BindLoginCredentialsPatchForEC2 extends AbstractModule {

    @Override
    protected void configure() {
      bind(EC2PopulateDefaultLoginCredentialsForImageStrategy.class)
        .to(TakeLoginCredentialsFromWhirrProperties.class);
    }
     
  }


}
