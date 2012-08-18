/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.puppet.predicates;

import static org.apache.whirr.service.puppet.PuppetConstants.MODULE_SOURCE_SUBKEY;
import static org.apache.whirr.service.puppet.PuppetConstants.PUPPET;
import static org.apache.whirr.service.puppet.PuppetConstants.PUPPET_ROLE_PREFIX;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * 
 */
public class PuppetPredicates {
  public static Predicate<String> isFirstPuppetRoleIn(final Iterable<String> roles) {
    return new Predicate<String>() {

      @Override
      public boolean apply(String arg0) {
        return Iterables.get(Iterables.filter(roles, Predicates.containsPattern("^" + PUPPET_ROLE_PREFIX + arg0)),
              0).equals(PUPPET_ROLE_PREFIX + arg0);
      }

      @Override
      public String toString() {
        return "isFirstPuppetRoleIn(" + roles + ")";

      }
    };

  }

  public static Predicate<CharSequence> isPuppetRole() {
    return Predicates.containsPattern("^" + PUPPET_ROLE_PREFIX);
  }

  public static Predicate<String> isLastPuppetRoleIn(final Iterable<String> roles) {
    return new Predicate<String>() {

      @Override
      public boolean apply(String arg0) {
        return Iterables.getLast(
              Iterables.filter(roles, Predicates.containsPattern("^" + PUPPET_ROLE_PREFIX + arg0))).equals(
              PUPPET_ROLE_PREFIX + arg0);
      }

      @Override
      public String toString() {
        return "isLastPuppetRoleIn(" + roles + ")";

      }
    };

  }
  

  public static Predicate<String> isModuleSubKey(final String module) {
    return new Predicate<String>() {

      @Override
      public boolean apply(String arg0) {
        return arg0.startsWith(PUPPET + "." + module + "." + MODULE_SOURCE_SUBKEY);
      }

      @Override
      public String toString() {
        return "isModuleSubKey(" + module + ")";

      }
    };
  }
}

