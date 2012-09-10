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
package org.apache.whirr.service.hbase.osgi;

import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.hbase.HBaseAvroServerClusterActionHandler;
import org.apache.whirr.service.hbase.HBaseMasterClusterActionHandler;
import org.apache.whirr.service.hbase.HBaseRegionServerClusterActionHandler;
import org.apache.whirr.service.hbase.HBaseRestServerClusterActionHandler;
import org.apache.whirr.service.hbase.HBaseThriftServerClusterActionHandler;
import org.jclouds.scriptbuilder.functionloader.osgi.BundleFunctionLoader;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import java.util.Properties;

public class Activator implements BundleActivator {

  private BundleFunctionLoader functionLoader;

  private final ClusterActionHandler masterClusterActionHandler = new HBaseMasterClusterActionHandler();
  private ServiceRegistration masterRegistration;

  private final ClusterActionHandler regionServerClusterActionHandler = new HBaseRegionServerClusterActionHandler();
  private ServiceRegistration regionServerRegistration;

  private final ClusterActionHandler restServerClusterActionHandler = new HBaseRestServerClusterActionHandler();
  private ServiceRegistration restServerRegistration;

  private final ClusterActionHandler avroServerClusterActionHandler = new HBaseAvroServerClusterActionHandler();
  private ServiceRegistration avroServerRegistration;

  private final ClusterActionHandler thriftServerClusterActionHandler = new HBaseThriftServerClusterActionHandler();
  private ServiceRegistration thriftServerRegistration;

  /**
   * Called when this bundle is started so the Framework can perform the
   * bundle-specific activities necessary to start this bundle. This method
   * can be used to register services or to allocate any resources that this
   * bundle needs.
   * <p/>
   * <p/>
   * This method must complete and return to its caller in a timely manner.
   *
   * @param context The execution context of the bundle being started.
   * @throws Exception If this method throws an exception, this
   *                   bundle is marked as stopped and the Framework will remove this
   *                   bundle's listeners, unregister all services registered by this
   *                   bundle, and release all services used by this bundle.
   */
  @Override
  public void start(BundleContext context) throws Exception {
    //Initialize OSGi based FunctionLoader
    functionLoader = new BundleFunctionLoader(context);
    functionLoader.start();

    Properties masterProps = new Properties();
    masterProps.put("name", "hbase-master");
    masterRegistration = context.registerService(ClusterActionHandler.class.getName(), masterClusterActionHandler, masterProps);

    Properties regionServerProps = new Properties();
    regionServerProps.put("name", "hbase-regionserver");
    regionServerRegistration = context.registerService(ClusterActionHandler.class.getName(), regionServerClusterActionHandler, regionServerProps);

    Properties restServerProps = new Properties();
    restServerProps.put("name", "hbase-restserver");
    restServerRegistration = context.registerService(ClusterActionHandler.class.getName(), restServerClusterActionHandler, restServerProps);

    Properties avroServerProps = new Properties();
    avroServerProps.put("name", "hbase-avroserver");
    avroServerRegistration = context.registerService(ClusterActionHandler.class.getName(), avroServerClusterActionHandler, avroServerProps);

    Properties thriftServerProps = new Properties();
    thriftServerProps.put("name", "hbase-thriftserver");
    thriftServerRegistration = context.registerService(ClusterActionHandler.class.getName(), thriftServerClusterActionHandler, thriftServerProps);
  }

  /**
   * Called when this bundle is stopped so the Framework can perform the
   * bundle-specific activities necessary to stop the bundle. In general, this
   * method should undo the work that the <code>BundleActivator.start</code>
   * method started. There should be no active threads that were started by
   * this bundle when this bundle returns. A stopped bundle must not call any
   * Framework objects.
   * <p/>
   * <p/>
   * This method must complete and return to its caller in a timely manner.
   *
   * @param context The execution context of the bundle being stopped.
   * @throws Exception If this method throws an exception, the
   *                   bundle is still marked as stopped, and the Framework will remove
   *                   the bundle's listeners, unregister all services registered by the
   *                   bundle, and release all services used by the bundle.
   */
  @Override
  public void stop(BundleContext context) throws Exception {
    if (masterRegistration != null) {
      masterRegistration.unregister();
    }
    if (regionServerRegistration != null) {
      regionServerRegistration.unregister();
    }

    if (restServerRegistration != null) {
      restServerRegistration.unregister();
    }
    if (avroServerRegistration != null) {
      avroServerRegistration.unregister();
    }
    if (thriftServerRegistration != null) {
      thriftServerRegistration.unregister();
    }
    if (functionLoader != null) {
      functionLoader.stop();
    }
  }
}
