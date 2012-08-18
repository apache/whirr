/*
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

package org.apache.whirr.karaf.itest.integration;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.karaf.itest.WhirrKarafTestSupport;
import org.osgi.framework.ServiceReference;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Iterator;
import java.util.Properties;

public class WhirrLiveTestSupport extends WhirrKarafTestSupport {

  public static final String ZOOKEEPER_RECIPE_PID = "org.apache.whirr.recipes.zookeeper";

  String provider;
  String identity;
  String credential;

  ConfigurationAdmin configurationAdmin;
  ServiceReference configurationAdminReference;

  public void lookupConfigurationAdmin() {
    configurationAdminReference = bundleContext.getServiceReference(ConfigurationAdmin.class.getName());
    configurationAdmin = (ConfigurationAdmin) bundleContext.getService(configurationAdminReference);
  }

  public void releaseConfigurationAdmin() {
    if (configurationAdminReference != null) {
      bundleContext.ungetService(configurationAdminReference);
    }
  }

  /**
   * Checks if Live Test parameters are properly configured.
   *
   * @return
   */
  public boolean isLiveConfigured() {
    provider = System.getProperty("whirr.test.provider");
    identity = System.getProperty("whirr.test.identity");
    credential = System.getProperty("whirr.test.credential");

    return provider != null & identity != null && credential != null
        && !provider.isEmpty() && !identity.isEmpty() && !credential.isEmpty()
        && (provider.equals("aws-ec2") || provider.equals("cloudservers-us"));
  }


  public void loadConfiguration(String name, String pid) throws ConfigurationException, IOException {
    if (configurationAdmin == null) {
      String message = "No configuration admin available. Make sure you have " +
          "properly looked it up before invoking this method";
      System.err.println(message);
      throw new IllegalStateException(message);
    }

    PropertiesConfiguration properties = new PropertiesConfiguration(getClass().getClassLoader().getResource(name));
    Configuration configuration = configurationAdmin.getConfiguration(pid);

    Dictionary dictionary = configuration.getProperties();
    if (dictionary == null) {
      dictionary = new Properties();
    }

    Iterator iterator = properties.getKeys();
    while (iterator.hasNext()) {
      String key = (String) iterator.next();
      String value = properties.getString(key);
      dictionary.put(key, value);
    }
    configuration.update(dictionary);
  }
}
