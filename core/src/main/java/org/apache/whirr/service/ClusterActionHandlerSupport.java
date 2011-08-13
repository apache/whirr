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
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.jclouds.RunUrlStatement;
import org.apache.whirr.util.BlobCache;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a utility class to make it easier to implement
 * {@link ClusterActionHandler}. For each 'before' and 'after' action type there
 * is a corresponding method that implementations may override.
 */
public abstract class ClusterActionHandlerSupport extends ClusterActionHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(ClusterActionHandler.class);

  @Override
  public void beforeAction(ClusterActionEvent event)
      throws IOException, InterruptedException{
    if (event.getAction().equals(BOOTSTRAP_ACTION)) {
      beforeBootstrap(event);
    } else if (event.getAction().equals(CONFIGURE_ACTION)) {
      beforeConfigure(event);
    } else if (event.getAction().equals(DESTROY_ACTION)) {
      beforeDestroy(event);
    } else {
      beforeOtherAction(event);
    }
  }

  @Override
  public void afterAction(ClusterActionEvent event)
      throws IOException, InterruptedException {
    if (event.getAction().equals(BOOTSTRAP_ACTION)) {
      afterBootstrap(event);
    } else if (event.getAction().equals(CONFIGURE_ACTION)) {
      afterConfigure(event);
    } else if (event.getAction().equals(DESTROY_ACTION)) {
      afterDestroy(event);
    } else {
      afterOtherAction(event);
    }
  }
  
  protected void beforeBootstrap(ClusterActionEvent event)
    throws IOException, InterruptedException { }
  
  protected void beforeConfigure(ClusterActionEvent event)
    throws IOException, InterruptedException { }

  protected void beforeDestroy(ClusterActionEvent event)
    throws IOException, InterruptedException { }

  protected void beforeOtherAction(ClusterActionEvent event)
    throws IOException, InterruptedException { }
  
  protected void afterBootstrap(ClusterActionEvent event)
    throws IOException, InterruptedException { }
  
  protected void afterConfigure(ClusterActionEvent event)
    throws IOException, InterruptedException { }
  
  protected void afterDestroy(ClusterActionEvent event)
    throws IOException, InterruptedException { }

  protected void afterOtherAction(ClusterActionEvent event)
    throws IOException, InterruptedException { }
  
  /**
   * Returns a composite configuration that is made up from the global
   * configuration coming from the Whirr core with the service default
   * properties.
   *
   * @param clusterSpec  The cluster specification instance.
   * @return The composite configuration.
   */
  protected Configuration getConfiguration(
      ClusterSpec clusterSpec, Configuration defaults) {
    CompositeConfiguration cc = new CompositeConfiguration();
    cc.addConfiguration(clusterSpec.getConfiguration());
    cc.addConfiguration(defaults);
    return cc;
  }

  protected Configuration getConfiguration(ClusterSpec clusterSpec,
      String defaultsPropertiesFile) throws IOException {
    try {
      return getConfiguration(clusterSpec,
          new PropertiesConfiguration(defaultsPropertiesFile));
    } catch(ConfigurationException e) {
      throw new IOException("Error loading " + defaultsPropertiesFile, e);
    }
 }
  
  /**
   * A convenience method for adding a {@link RunUrlStatement} to a
   * {@link ClusterActionEvent}.
   */
  public static void addRunUrl(ClusterActionEvent event, String runUrl,
      String... args)
      throws IOException {
    Statement statement = new RunUrlStatement(
        event.getClusterSpec().getRunUrlBase(), runUrl, args);
    addStatement(event, statement);
  }

  public static void addStatement(ClusterActionEvent event, Statement statement) {
    event.getStatementBuilder().addStatement(statement);
  }

  /**
   * Prepare the file url for the remote machine.
   *
   * For public urls this function does nothing. For local urls it uploads the files to a
   * temporary blob cache and adds download statement.
   *
   * @param rawUrl    raw url as provided in the configuration file
   * @return  an URL visible to the install / configure scripts
   */
  public String prepareRemoteFileUrl(ClusterActionEvent event, String rawUrl)
      throws IOException {
    if (rawUrl != null && rawUrl.startsWith("file://")) {
      try {
        URI uri = new URI(rawUrl);
        File localFile = new File(uri);

        BlobCache cache = BlobCache.getInstance(event.getCompute(), event.getClusterSpec());
        cache.putIfAbsent(localFile);

        final String basePath = "/tmp/whirr/cache/files/";
        addStatement(event, cache.getAsSaveToStatement(basePath, uri));
        return "file://" + basePath + localFile.getName();

      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
    } else if(rawUrl != null && rawUrl.startsWith("remote://")) {
      return rawUrl.replaceFirst("remote://", "file://");
    }
    return rawUrl;
  }

  /**
   * Get service start function name from the configuration
   */
  public String getStartFunction(Configuration config, String service, String defaultFunction) {
    return getFunctionName(config, service, "start", defaultFunction);
  }

  /**
   * Get service start function name from the configuration
   */
  public String getStopFunction(Configuration config, String service, String defaultFunction) {
    return getFunctionName(config, service, "stop", defaultFunction);
  }

  /**
   * Get service install function name from the configuration
   */
  public String getInstallFunction(Configuration config, String service, String defaultFunction) {
    return getFunctionName(config, service, "install", defaultFunction);
  }

  /**
   * Get service configure function name from the configuration
   */
  public String getConfigureFunction(Configuration config, String service, String defaultFunction) {
    return getFunctionName(config, service, "configure", defaultFunction);
  }

  /**
   * Get service cleanup function name from the configuration
   */
  public String getCleanupFunction(Configuration config, String service, String defaultFunction) {
    return getFunctionName(config, service, "cleanup", defaultFunction);
  }

  public String getFunctionName(Configuration config, String service, String functionName, String defaultFunction) {

    String deprecatedKey = String.format("whirr.%s-%s-function", service, functionName);
    String key = String.format("whirr.%s.%s-function", service, functionName);

    if (config.containsKey(deprecatedKey)) {
      LOG.warn("'{}' is deprecated. Replace with '{}'", deprecatedKey, key);
      return config.getString(deprecatedKey);
    }

    return config.getString(key, defaultFunction);
  }

}
