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

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;

import java.util.Collection;

import org.jclouds.compute.domain.NodeMetadata;

/*
 * Currently unused.
 * <p>
 * Idea was to encode role in the tag. However, for simplicity we use a tag for
 * all instances in the cluster, even those in different roles.
 * If roles are in the tag, then in EC2 the roles are in different security groups
 * so they can't communicate with each other without further work on firewall rules.
 * The downside to not encoding role in the tag is that you can't tell an instance's
 * role by looking at its name or group. Also, not sure you can have different
 * firewall rules for each role (i.e. we get the union of rules for each role).
 * Metadata for the cluster needs to be stored outside the cluster.
 * This is slightly less convenient, but makes cross-provider support easier to achieve
 * (since tags have different semantics on different providers. E.g. terremark tag is up to 15 chars)
 * In future we could provide convenience methods for storing metadata on local filesystem,
 * ZooKeeper, MySQL, etc.
 */
public class TagBuilder {

  public static String tagFromRoles(String clusterName, String... roleAbbreviations) {
    return clusterName + "_" + Joiner.on('+').join(roleAbbreviations);
  }
  
  public static Iterable<String> rolesFromTag(String tag) {
    return Splitter.on('+').split(tag.substring(tag.lastIndexOf('_') + 1));
  }
  
  public static <T extends NodeMetadata> NodeMetadata getSingleNodeInRole(final String roleAbreviation, Collection<T> nodes) {
    return Iterables.getOnlyElement(Collections2.filter(nodes, new Predicate<NodeMetadata>() {
      @Override
      public boolean apply(NodeMetadata metadata) {
        return Iterables.contains(rolesFromTag(metadata.getTag()), roleAbreviation);
      }
    }));
  }

}
