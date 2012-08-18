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

package org.apache.whirr.service.puppet.functions;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.contains;
import static com.google.common.collect.Iterables.find;
import static java.lang.String.format;
import static org.apache.whirr.service.puppet.PuppetConstants.MODULES_DIR;
import static org.apache.whirr.service.puppet.PuppetConstants.MODULE_KEY_PATTERN;
import static org.apache.whirr.service.puppet.PuppetConstants.PUPPET;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.puppet.statements.InstallModuleFromGit;
import org.jclouds.scriptbuilder.domain.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;

public class StatementToInstallModule implements Function<Map<String, String>, Statement> {
  static final Logger LOG = LoggerFactory.getLogger(StatementToInstallModule.class);

  private final PrepareRemoteFileUrl prepareRemoteFileUrl;

  @VisibleForTesting
  public interface PrepareRemoteFileUrl {
    /**
     * Prepare the file url for the remote machine.
     * 
     * For public urls this function does nothing. For local urls it uploads the files to a
     * temporary blob cache and adds download statement.
     * 
     * @param rawUrl
     *        raw url as provided in the configuration file
     * @return an URL visible to the install / configure scripts
     * @throws IOException
     */
    String apply(String rawUrl) throws IOException;
  }

  public static class PrepareRemoteFileUrlUsingBlobCache implements PrepareRemoteFileUrl {

    private final ClusterActionEvent event;

    public PrepareRemoteFileUrlUsingBlobCache(ClusterActionEvent event) {
      this.event = checkNotNull(event, "event");

    }

    /**
     * @see ClusterActionHandlerSupport#prepareRemoteFileUrl
     */
    @Override
    public String apply(String rawUrl) throws IOException {
      return ClusterActionHandlerSupport.prepareRemoteFileUrl(event, rawUrl);
    }

  }

  public StatementToInstallModule(ClusterActionEvent event) {
    this(new PrepareRemoteFileUrlUsingBlobCache(event));
  }

  public StatementToInstallModule(PrepareRemoteFileUrl prepareRemoteFileUrl) {
    this.prepareRemoteFileUrl = checkNotNull(prepareRemoteFileUrl, "prepareRemoteFileUrl");
  }

  @Override
  public Statement apply(Map<String, String> props) {
    try {
      return installModuleFromRemoteFileOrGit(find(props.keySet(), contains(MODULE_KEY_PATTERN)), props);
    } catch (NoSuchElementException e) {
      throw new IllegalArgumentException(format("couldn't find pattern: %s in properties %s", MODULE_KEY_PATTERN,
            props));
    }
  }

  private Statement installModuleFromRemoteFileOrGit(String roleKey, Map<String, String> props) {
    String moduleName = roleKey.substring(PUPPET.length() + 1).replaceAll("\\..*", "");
    URI srcUri = URI.create(props.get(roleKey));
    if ("git".equals(srcUri.getScheme())) {
      return new InstallModuleFromGit(moduleName, srcUri, props.get(roleKey + ".branch"));
    } else {
      try {
        String remotelyAccessibleUrl = prepareRemoteFileUrl.apply(srcUri.toASCIIString());
        return call("install_tarball", remotelyAccessibleUrl, MODULES_DIR + moduleName);
      } catch (IOException e) {
        throw new IllegalArgumentException("problem getting src: " + srcUri, e);
      }
    }
  }

}
