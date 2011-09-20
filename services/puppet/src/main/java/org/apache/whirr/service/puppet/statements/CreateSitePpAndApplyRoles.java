/**
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.whirr.service.puppet.statements;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.whirr.service.puppet.PuppetConstants.SITE_PP_FILE_LOCATION;
import static org.jclouds.scriptbuilder.domain.Statements.appendFile;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.service.puppet.Manifest;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.jclouds.scriptbuilder.domain.Statement;
import org.jclouds.scriptbuilder.domain.StatementList;
import org.jclouds.scriptbuilder.domain.Statements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableList.Builder;

public class CreateSitePpAndApplyRoles implements Statement {
  private Iterable<String> roles;
  private Configuration config;

  public CreateSitePpAndApplyRoles(Iterable<String> roles, Configuration config) {
    this.roles = checkNotNull(roles, "roles");
    this.config = checkNotNull(config, "config");
  }

  @Override
  public Iterable<String> functionDependencies(OsFamily arg0) {
    return ImmutableSet.of();
  }

  @Override
  public String render(OsFamily arg0) {

    // when we get to the last role, let's cat all the manifests we made together inside a
    // node default site.pp
    Builder<Statement> statements = ImmutableList.<Statement> builder();

    statements.add(Statements.rm(SITE_PP_FILE_LOCATION));
    Builder<String> sitePp = ImmutableList.<String> builder();

    sitePp.add("node default {");
    for (String role : roles) {
      String manifestAttribPrefix = role.replaceAll(":+", ".");
      Configuration manifestProps = new PropertiesConfiguration();
      for (@SuppressWarnings("unchecked")
      Iterator<String> it = config.getKeys(manifestAttribPrefix); it.hasNext();) {
        String key = it.next();
        manifestProps.setProperty(key, config.getProperty(key));
      }
      sitePp.add(getManifestForClusterSpecAndRole(role, manifestProps).toString());
    }
    sitePp.add("}");

    statements.add(appendFile(SITE_PP_FILE_LOCATION, sitePp.build()));
    statements.add(exec("puppet apply " + SITE_PP_FILE_LOCATION));

    return new StatementList(statements.build()).render(arg0);
  }

  static final Logger LOG = LoggerFactory.getLogger(CreateSitePpAndApplyRoles.class);

  // TODO refactor this
  @SuppressWarnings("unchecked")
  @VisibleForTesting
  static Manifest getManifestForClusterSpecAndRole(String subroleModuleManifest, Configuration manifestProps) {
    int firstColon = subroleModuleManifest.indexOf(':');
    String moduleName, manifestClassName;
    if (firstColon == -1) {
      moduleName = subroleModuleManifest;
      manifestClassName = null;
    } else {
      moduleName = subroleModuleManifest.substring(0, firstColon);
      int firstDoubleColon = subroleModuleManifest.indexOf("::");
      if (firstDoubleColon != firstColon)
        throw new IllegalArgumentException("Malformed subrole spec for role puppet: "
              + "format should be puppet:module or puppet:module::manifest");
      manifestClassName = subroleModuleManifest.substring(firstDoubleColon + 2);
    }

    // now create and populate the manifest
    Manifest manifest = new Manifest(moduleName, manifestClassName);

    for (Iterator<?> longkeyI = manifestProps.getKeys(); longkeyI.hasNext();) {
      String longkey = (String) longkeyI.next();
      String key = longkey.substring(subroleModuleManifest.replaceAll(":+", ".").length() + 1);
      if (key.indexOf('.') >= 0) {
        // it's for a sub-manifest; skip it
      } else {
        Object value = manifestProps.getProperty(longkey);
        // an array, e.g. ['1', '2'] gets parsed as a list of two strings, which we need to join
        // with ", "
        String vs = "";
        if (value == null)
          LOG.warn("Invalid value for key " + longkey + ": null"); // shouldn't happen
        else if (value instanceof Collection) {
          Iterator<?> vi = ((Collection<?>) value).iterator();
          if (!vi.hasNext())
            LOG.warn("Invalid value for key " + longkey + ": empty list"); // shouldn't happen
          else {
            vs += vi.next();
            while (vi.hasNext())
              vs += ", " + vi.next();
          }
        } else {
          vs = value.toString();
        }
        manifest.attribs.put(key, vs);
      }
    }
    LOG.debug("Bootstrapping " + subroleModuleManifest + ", produced manifest:\n" + manifest);
    return manifest;
  }
}
