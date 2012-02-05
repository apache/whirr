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

package org.apache.whirr.karaf.command.support;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.osgi.service.cm.Configuration;
import org.osgi.service.cm.ConfigurationAdmin;

import java.io.IOException;
import java.util.Arrays;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.List;

public class ConfigurationReader {

  public static final String WHIRR_IDENTIY = "whirr.identity";
  public static final String WHIRR_CREDENTIAL = "whirr.credential";
  public static final String WHIRR_PROVIDER = "whirr.provider";
  public static final String WHIRR_CLUSTER_NAME = "whirr.cluster-name";
  public static final String DEFAULT_CLUSTER_NAME = "default";

  private ConfigurationReader() {
    //Utility Class
  }


   /**
   * Builds Configuration from a File.
   *
   * @param fileName
   * @return
   * @throws org.apache.commons.configuration.ConfigurationException
   */
  public static PropertiesConfiguration fromFile(String fileName) {
    PropertiesConfiguration config = null;
    try {
      config = new PropertiesConfiguration(fileName);
    } catch (ConfigurationException e) {
      System.err.println(String.format("Failed to read configuration from file:%s.%s.", fileName, e.getMessage()));
    }
    return config;
  }

  /**
   * Builds the Configuration from Configuration Admin.
   *
   * @param pid
   * @return
   */
  public static PropertiesConfiguration fromConfigAdmin(ConfigurationAdmin configurationAdmin, String pid) {
    List<String> ignoredKeys = Arrays.asList("service.pid", "felix.fileinstall.filename");
    PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration();
    try {
      Configuration configuration = configurationAdmin.getConfiguration(pid);
      Dictionary properties = configuration.getProperties();
      Enumeration keys = properties.keys();
      while (keys.hasMoreElements()) {
        Object key = keys.nextElement();
        Object value = properties.get(key);
        if (!ignoredKeys.contains(key)) {
          propertiesConfiguration.addProperty(String.valueOf(key), value);
        }
      }
    } catch (IOException e) {
      System.err.println(String.format("No configuration found for pid:%s.", pid));
      return null;
    }
    return propertiesConfiguration;
  }
}
