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

package org.apache.whirr;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;

import org.apache.whirr.Cluster.Instance;

import javax.annotation.Nullable;

/**
 * {@link Predicate}s for matching {@link Instance}s with certain cluster roles.
 */
public class RolePredicates {

  /**
   * @param role
   * @return A {@link Predicate} that matches {@link Instance}s whose roles
   * include <code>role</code>.
   */
  public static Predicate<Instance> role(final String role) {
    return allRolesIn(Collections.singleton(role));
  }

  /**
   * @param roles
   * @return A {@link Predicate} that matches {@link Instance}s whose roles
   * are exactly the same as those in <code>roles</code>.
   */
  public static Predicate<Instance> onlyRolesIn(final Set<String> roles) {
    return new Predicate<Instance>() {
      @Override
      public boolean apply(Instance instance) {
        return instance.getRoles().equals(roles);
      }
    };
  }
  
  /**
   * @param roles
   * @return A {@link Predicate} that matches {@link Instance}s whose roles
   * contain all of <code>roles</code>.
   */
  public static Predicate<Instance> allRolesIn(final Set<String> roles) {
    return new Predicate<Instance>() {
      @Override
      public boolean apply(Instance instance) {
        return instance.getRoles().containsAll(roles);
      }
    };
  }

  /**
   * @param roles
   * @return A {@link Predicate} that matches {@link Instance}s whose roles
   * contain at least one of <code>roles</code>.
   */
  public static Predicate<Instance> anyRoleIn(final Set<String> roles) {
    return new Predicate<Instance>() {
      @Override
      public boolean apply(Instance instance) {
        Set<String> copy = Sets.newLinkedHashSet(instance.getRoles());
        copy.retainAll(roles);
        return !copy.isEmpty();
      }
    };
  }

  /**
   *
   * @param ids list of instance ids
   * @return  A {@link Predicate} that matches {@link Instance} whose id is
   * found in the IDs list
   */
  public static Predicate<Instance> withIds(String ...ids) {
    return withIds(Sets.<String>newHashSet(ids));
  }

  /**
   *
   * @param ids list of instance ids
   * @return  A {@link Predicate} that matches {@link Instance} whose id is
   * found in the IDs list
   */
  public static Predicate<Instance> withIds(final Set<String> ids) {
    return new Predicate<Instance>() {
      @Override
      public boolean apply(@Nullable Instance input) {
        return (input != null) ? ids.contains(input.getId()) : false;
      }
    };
  }


}
