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

package org.apache.whirr.osgi;

import org.apache.whirr.ByonClusterController;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterControllerFactory;
import org.apache.whirr.DynamicClusterControllerFactory;
import org.apache.whirr.DynamicHandlerMapFactory;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.DynamicComputeCache;
import org.apache.whirr.state.ClusterStateStoreFactory;
import org.jclouds.compute.ComputeService;
import org.jclouds.scriptbuilder.functionloader.osgi.BundleFunctionLoader;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;

import java.util.Properties;


public class Activator implements BundleActivator {

  private ClusterStateStoreFactory clusterStateStoreFactory = new ClusterStateStoreFactory();

  private DynamicComputeCache dynamicComputeCache = new DynamicComputeCache();
  private ServiceTracker computeServiceTracker;

  private DynamicHandlerMapFactory handlerMapFactory = new DynamicHandlerMapFactory();
  private ServiceRegistration handlerMapFactoryRegistration;
  private ServiceTracker handlerTracker;

  private DynamicClusterControllerFactory clusterControllerFactory = new DynamicClusterControllerFactory();
  private ServiceRegistration clusterControllerFactoryRegistration;
  private ServiceTracker clusterControllerTracker;

  private ClusterController defaultClusterController = new ClusterController(dynamicComputeCache, clusterStateStoreFactory);
  private ServiceRegistration defaultClusterControllerRegistration;

  private ClusterController byonClusterController = new ByonClusterController(dynamicComputeCache, clusterStateStoreFactory);
  private ServiceRegistration byonClusterControllerRegistration;


  private BundleFunctionLoader functionLoader;

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

    defaultClusterController.setHandlerMapFactory(handlerMapFactory);
    byonClusterController.setHandlerMapFactory(handlerMapFactory);

        //Register services
    clusterControllerFactoryRegistration = context.registerService(ClusterControllerFactory.class.getName(), clusterControllerFactory, null);
    handlerMapFactoryRegistration = context.registerService(DynamicHandlerMapFactory.class.getName(), handlerMapFactory, null);

    //Start tracking
    clusterControllerTracker = new ServiceTracker(context, ClusterController.class.getName(), null) {

      @Override
      public Object addingService(ServiceReference reference) {
        Object service = super.addingService(reference);
        clusterControllerFactory.bind((ClusterController) service);
        return service;
      }

      @Override
      public void removedService(ServiceReference reference, Object service) {
        clusterControllerFactory.unbind((ClusterController) service);
        super.removedService(reference, service);
      }
    };

    clusterControllerTracker.open();

    computeServiceTracker = new ServiceTracker(context, ComputeService.class.getName(), null) {

      @Override
      public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        dynamicComputeCache.bind((ComputeService) service);
        return service;
      }

      @Override
      public void removedService(ServiceReference reference, Object service) {
        dynamicComputeCache.unbind((ComputeService) service);
        super.removedService(reference, service);
      }
    };

    computeServiceTracker.open();


    handlerTracker = new ServiceTracker(context, ClusterActionHandler.class.getName(), null) {

      @Override
      public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        handlerMapFactory.bind((ClusterActionHandler) service);
        return service;
      }

      @Override
      public void removedService(ServiceReference reference, Object service) {
        handlerMapFactory.unbind((ClusterActionHandler) service);
        super.removedService(reference, service);
      }
    };

    handlerTracker.open();

    Properties defaultClusterControllerProperties = new Properties();
    defaultClusterControllerProperties.setProperty("name", "default");
    defaultClusterControllerRegistration = context.registerService(ClusterController.class.getName(), defaultClusterController, defaultClusterControllerProperties);

    Properties byonClusterControllerProperties = new Properties();
    byonClusterControllerProperties.setProperty("name", "byon");
    byonClusterControllerRegistration = context.registerService(ClusterController.class.getName(), byonClusterController, byonClusterControllerProperties);
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
    if (functionLoader != null) {
      functionLoader.stop();
    }

    if (handlerMapFactoryRegistration != null) {
      handlerMapFactoryRegistration.unregister();
    }

    if (clusterControllerFactoryRegistration != null) {
      clusterControllerFactoryRegistration.unregister();
    }

    if (clusterControllerTracker != null) {
      clusterControllerTracker.close();
    }

    if (handlerTracker != null) {
      handlerTracker.close();
    }
  }
}
