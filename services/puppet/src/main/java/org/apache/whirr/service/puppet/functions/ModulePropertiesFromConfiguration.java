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

package org.apache.whirr.service.puppet.functions;

import static org.apache.whirr.service.puppet.PuppetConstants.MODULE_SOURCE_SUBKEY;
import static org.apache.whirr.service.puppet.PuppetConstants.PUPPET;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableMap.Builder;

@SuppressWarnings("unchecked")
public enum ModulePropertiesFromConfiguration implements Function<Configuration, Map<String, String>> {
  INSTANCE;
  public Map<String, String> apply(final Configuration config) {
    Iterator<String> allKeys = Iterators.transform(config.getKeys(), Functions.toStringFunction());
    Set<String> moduleKeys = Sets.newHashSet(Iterators.<String> filter(allKeys, Predicates.and(Predicates
          .containsPattern(PUPPET + "\\.[^.]+\\." + MODULE_SOURCE_SUBKEY), new Predicate<String>() {

      @Override
      public boolean apply(String arg0) {
        // TODO not sure that we have to check this
        return config.getString(arg0, null) != null;
      }

    })));
    Builder<String, String> builder = ImmutableMap.<String, String> builder();
    for (String key : moduleKeys) {
      builder.put(key, config.getString(key));
    }
    return builder.build();
  }
}
