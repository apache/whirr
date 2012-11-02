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
package org.apache.whirr.service.kerberos.osgi;

import java.util.Properties;

import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.kerberos.KerberosClientHandler;
import org.apache.whirr.service.kerberos.KerberosServerHandler;
import org.jclouds.scriptbuilder.functionloader.osgi.BundleFunctionLoader;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

public class Activator implements BundleActivator {

  private BundleFunctionLoader functionLoader;

  private final ClusterActionHandler clientActionHandler = new KerberosClientHandler();
  private ServiceRegistration clientRegistration;

  private final ClusterActionHandler serverClusterActionHandler = new KerberosServerHandler();
  private ServiceRegistration serverRegistration;

  @Override
  public void start(BundleContext context) throws Exception {
    functionLoader = new BundleFunctionLoader(context);
    functionLoader.start();
    Properties clientProps = new Properties();
    clientProps.put("name", KerberosClientHandler.ROLE);
    clientRegistration = context
      .registerService(ClusterActionHandler.class.getName(), clientActionHandler, clientProps);
    Properties serverProps = new Properties();
    serverProps.put("name", KerberosServerHandler.ROLE);
    serverRegistration = context.registerService(ClusterActionHandler.class.getName(), serverClusterActionHandler,
      serverProps);
  }

  @Override
  public void stop(BundleContext context) throws Exception {
    if (clientRegistration != null) {
      clientRegistration.unregister();
    }
    if (serverRegistration != null) {
      serverRegistration.unregister();
    }
    if (functionLoader != null) {
      functionLoader.stop();
    }
  }
}
