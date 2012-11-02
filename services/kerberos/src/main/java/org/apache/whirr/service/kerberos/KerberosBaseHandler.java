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
package org.apache.whirr.service.kerberos;

import static org.apache.whirr.RolePredicates.role;
import static org.jclouds.scriptbuilder.domain.Statements.call;

import java.io.IOException;
import java.util.NoSuchElementException;

import org.apache.commons.configuration.Configuration;
import org.apache.whirr.Cluster.Instance;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandlerSupport;

public abstract class KerberosBaseHandler extends ClusterActionHandlerSupport {

  protected Configuration getConfiguration(ClusterSpec spec) throws IOException {
    return getConfiguration(spec, "whirr-kerberos-default.properties");
  }

  @Override
  protected void beforeBootstrap(ClusterActionEvent event) throws IOException {
    addStatement(event, call("configure_hostnames"));
    addStatement(event, call("retry_helpers"));
    addStatement(event, call(getInstallFunction(event.getClusterSpec().getConfiguration(), "java", "install_openjdk")));
    addStatement(event, call("install_kerberos_client"));
  }

  @Override
  protected void beforeConfigure(ClusterActionEvent event) throws IOException {
    Instance kerberosServerInstance = null;
    try {
      kerberosServerInstance = event.getCluster().getInstanceMatching(role(KerberosServerHandler.ROLE));
    } catch (NoSuchElementException noSuchElementException) {
      // ignore exception which a indicates client role has been configured
      // without a cluster KDC role, but ensure the configure client script is
      // not executed, which depends on the KDC hostname
    }
    if (kerberosServerInstance != null) {
      addStatement(event, call("configure_kerberos_client", "-h", kerberosServerInstance.getPublicHostName()));
    }
  }
}
