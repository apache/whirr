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

package org.apache.whirr.service.hadoop;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.configuration.AbstractConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.Statements;

/**
 * Helper class to convert between Hadoop configuration representations.
 */
public class HadoopConfigurationConverter {
  
  private static final String FINAL_SUFFIX = ".final";

  @VisibleForTesting
  static List<String> asXmlConfigurationLines(Configuration hadoopConfig) {
    List<String> lines = Lists.newArrayList();
    lines.add("<configuration>");
    for (@SuppressWarnings("unchecked")
        Iterator<String> it = hadoopConfig.getKeys(); it.hasNext(); ) {
      String key = it.next();
      if (key.endsWith(FINAL_SUFFIX)) {
        continue;
      }

      // rebuild the original value by joining all of them with the default separator
      String value = StringUtils.join(hadoopConfig.getStringArray(key),
          AbstractConfiguration.getDefaultListDelimiter());
      lines.add("  <property>");
      lines.add(String.format("    <name>%s</name>", key));
      lines.add(String.format("    <value>%s</value>", value));
      String finalValue = hadoopConfig.getString(key + FINAL_SUFFIX);
      if (finalValue != null) {
        lines.add(String.format("    <final>%s</final>", finalValue));
      }
      lines.add("  </property>");
    }
    lines.add("</configuration>");
    return lines;
  }
  
  public static Statement asCreateXmlConfigurationFileStatement(String path, 
      Configuration hadoopConfig) {
    return Statements.appendFile(path, asXmlConfigurationLines(hadoopConfig));
  }

  @VisibleForTesting
  static List<String> asEnvironmentVariablesLines(Configuration hadoopConfig) {
    List<String> lines = Lists.newArrayList();
    
    for (@SuppressWarnings("unchecked")
        Iterator<String> it = hadoopConfig.getKeys(); it.hasNext(); ) {
      String key = it.next();
      if (key.endsWith(FINAL_SUFFIX)) {
        continue;
      }

      // Write the export line. We only allow one value per key 
      String value = hadoopConfig.getString(key);
      lines.add(new StringBuilder("export ")
            .append(key)
            .append("=\"")
            .append(value)
            .append("\"").toString());
    }
    return lines;
  }
  
  public static Statement asCreateEnvironmentVariablesFileStatement(String path, 
      Configuration config) {
    return Statements.appendFile(path, asEnvironmentVariablesLines(config));
  }
  
}
