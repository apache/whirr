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

package org.apache.whirr.actions;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.HandlerMapFactory;
import org.apache.whirr.compute.NodeStarter;
import org.apache.whirr.compute.NodeStarterFactory;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ClusterActionHandlerFactory;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.Hardware;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeState;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.TemplateBuilder;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.domain.internal.HardwareImpl;
import org.jclouds.compute.domain.internal.ImageImpl;
import org.jclouds.compute.domain.internal.NodeMetadataImpl;
import org.jclouds.compute.domain.internal.TemplateImpl;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Credentials;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.internal.LocationImpl;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BootstrapClusterActionTest {

  private static final Logger LOG =
    LoggerFactory.getLogger(BootstrapClusterActionTest.class);

  @SuppressWarnings("unchecked")
@Test
  public void testDoActionRetriesSucceed() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.service-name", "test-service");
    conf.addProperty("whirr.cluster-name", "test-cluster");
    conf.addProperty("whirr.instance-templates", "1 jt+nn,4 dn+tt");
    conf.addProperty("whirr.instance-templates-max-percent-failures", "60 dn+tt");
    conf.addProperty("whirr.provider", "ec2");
    config.addConfiguration(conf);
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(conf);

    Set<String> jtnn = new HashSet<String>();
    jtnn.add("hadoop-jobtracker"); 
    jtnn.add("hadoop-namenode");     
    Set<String> dntt = new HashSet<String>();
    dntt.add("hadoop-datanode");
    dntt.add("hadoop-tasktracker");

    TestNodeStarterFactory nodeStarterFactory = null;
    
    ClusterActionHandler handler = mock(ClusterActionHandler.class);     
    Map<String, ClusterActionHandler> handlerMap = Maps.newHashMap();
    handlerMap.put("hadoop-jobtracker", handler);
    handlerMap.put("hadoop-namenode", handler);
    handlerMap.put("hadoop-datanode", handler);
    handlerMap.put("hadoop-tasktracker", handler);

    Function<ClusterSpec, ComputeServiceContext> getCompute = mock(Function.class);
    ComputeServiceContext serviceContext = mock(ComputeServiceContext.class);
    ComputeService computeService = mock(ComputeService.class);
    TemplateBuilder templateBuilder = mock(TemplateBuilder.class);
    Template template = mock(Template.class);

    when(getCompute.apply(clusterSpec)).thenReturn(serviceContext);
    when(serviceContext.getComputeService()).thenReturn(computeService);
    when(computeService.templateBuilder()).thenReturn(templateBuilder);
    when(templateBuilder.options((TemplateOptions) any())).thenReturn(templateBuilder);
    when(templateBuilder.build()).thenReturn(template);
    
    // here is a scenario when jt+nn fails once, then the retry is successful
    // and from the dn+tt one node fails, then the retry is successful
    Map<Set<String>, Stack<Integer>> reaction = Maps.newHashMap();
    Stack<Integer> jtnnStack = new Stack<Integer>();
    jtnnStack.push(new Integer(1)); // then ok
    jtnnStack.push(new Integer(0)); // initially fail
    reaction.put(jtnn, jtnnStack);
    Stack<Integer> ddttStack = new Stack<Integer>();
    ddttStack.push(new Integer(3)); // 3 from 4, just enough
    reaction.put(dntt, ddttStack);
    
    nodeStarterFactory = new TestNodeStarterFactory(reaction);
    BootstrapClusterAction bootstrapper =
        new BootstrapClusterAction(getCompute, handlerMap, nodeStarterFactory);
    
    bootstrapper.execute(clusterSpec, null);
    if (nodeStarterFactory != null) {
      nodeStarterFactory.validateCompletion();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(expected = IOException.class)
  public void testDoActionRetriesExceeds() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.service-name", "test-service");
    conf.addProperty("whirr.cluster-name", "test-cluster");
    conf.addProperty("whirr.instance-templates", "1 jt+nn,4 dn+tt");
    conf.addProperty("whirr.instance-templates-max-percent-failures", "60 dn+tt");
    conf.addProperty("whirr.provider", "ec2");
    config.addConfiguration(conf);
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(conf);

    Set<String> jtnn = new HashSet<String>();
    jtnn.add("hadoop-jobtracker"); 
    jtnn.add("hadoop-namenode");     
    Set<String> dntt = new HashSet<String>();
    dntt.add("hadoop-datanode");
    dntt.add("hadoop-tasktracker");

    TestNodeStarterFactory nodeStarterFactory = null;
    
    ClusterActionHandler handler = mock(ClusterActionHandler.class);     
    Map<String, ClusterActionHandler> handlerMap = Maps.newHashMap();
    handlerMap.put("hadoop-jobtracker", handler);
    handlerMap.put("hadoop-namenode", handler);
    handlerMap.put("hadoop-datanode", handler);
    handlerMap.put("hadoop-tasktracker", handler);

    Function<ClusterSpec, ComputeServiceContext> getCompute = mock(Function.class);
    ComputeServiceContext serviceContext = mock(ComputeServiceContext.class);
    ComputeService computeService = mock(ComputeService.class);
    TemplateBuilder templateBuilder = mock(TemplateBuilder.class);
    Template template = mock(Template.class);

    when(getCompute.apply(clusterSpec)).thenReturn(serviceContext);
    when(serviceContext.getComputeService()).thenReturn(computeService);
    when(computeService.templateBuilder()).thenReturn(templateBuilder);
    when(templateBuilder.options((TemplateOptions) any())).thenReturn(templateBuilder);
    when(templateBuilder.build()).thenReturn(template);
    
    // here is a scenario when jt+nn does not fail
    // but the dn+tt one node fails 3, then in the retry fails 2
    // at the end result, the cluster will fail, throwing IOException
    Map<Set<String>, Stack<Integer>> reaction = Maps.newHashMap();
    Stack<Integer> jtnnStack = new Stack<Integer>();
    jtnnStack.push(new Integer(1));
    reaction.put(jtnn, jtnnStack);
    Stack<Integer> ddttStack = new Stack<Integer>();
    ddttStack.push(new Integer(1));  // 1 from 4, retryRequired
    ddttStack.push(new Integer(1));  // 1 from 4, still retryRequired
    reaction.put(dntt, ddttStack);
    
    nodeStarterFactory = new TestNodeStarterFactory(reaction);
    BootstrapClusterAction bootstrapper = new BootstrapClusterAction(getCompute, handlerMap, nodeStarterFactory);
    
    bootstrapper.execute(clusterSpec, null); // this should file with too many retries
    if (nodeStarterFactory != null) {
      nodeStarterFactory.validateCompletion();
    }
  }
  
  /**
   * A factory which returns controllable Callables in order
   * to control the number of nodes returned.
   * The control plan specifies a stack of node numbers to return 
   * by role. At the end of a bootstrap process, the stacks has to
   * be emptied which validates the exact number of node creation attempts. 
   */
  class TestNodeStarterFactory extends NodeStarterFactory {
    
    private AtomicInteger id = new AtomicInteger(0);

    private final Map<Set<String>, Stack<Integer>> plan;
    
    TestNodeStarterFactory(final Map<Set<String>, Stack<Integer>> plan) {
      this.plan = plan;
    }

    @Override
    public NodeStarter create(final ComputeService computeService, final String clusterName,
        final Set<String> roles, final int num, final Template template) {
      NodeStarter result = null;
      Stack<Integer> stack = plan.get(roles);
      if (stack != null) {
        synchronized(stack) {
          Integer i = stack.pop();
          if (i != null) {
            result = new TestNodeStarter(computeService, clusterName, 
                roles, num, template, id.incrementAndGet(), i.intValue());
          }
        }
      }
      assertNotNull("Incorrect plan for " + roles, result);
      return result;
    }
    
    void validateCompletion() {
      for (Entry<Set<String>, Stack<Integer>> entry : plan.entrySet()) {
        Set<String> role = entry.getKey();
        Stack<Integer> stack = entry.getValue();
        assertEquals("Role " + role, 0, stack.size());
      }
    }    
  }
  
  /**
   * Callable which returns a controllable number of nodes.
   */
  class TestNodeStarter extends NodeStarter {
    
    private int id;
    private int num;
    private int only;
    private Set<String> roles;

    /**
     * TestNodeStarter
     * @param computeService
     * @param clusterName
     * @param roles
     * @param num - desired number of nodes to start
     * @param template
     * @param id - just to uniquely identify the node
     * @param only - only number of nodes to start
     */
    TestNodeStarter(ComputeService computeService, String clusterName, Set<String> roles,
        int num, Template template, int id, int only) {
      super(computeService, clusterName, roles, num, template);
      this.id = id;
      this.num = num;
      this.only = only;
      this.roles = roles;
    }
    
    @Override
    public Set<NodeMetadata> call() throws Exception {
      Map<String, String> userMetadata = Maps.newHashMap();
      Map<String, Object> locationMetadata = Maps.newHashMap();
      Location location = new LocationImpl(LocationScope.ZONE, "loc", "test location", 
        null, new ArrayList<String>(), locationMetadata);
      Set<String> addresses = Sets.newHashSet();
      addresses.add("10.0.0.1");
      Credentials loginCredentials = new Credentials("id", "cred");

      Set<NodeMetadata> nodes = Sets.newHashSet();
      Map<?, Exception> executionExceptions = Maps.newHashMap();
      Map<NodeMetadata, Throwable> failedNodes = Maps.newHashMap();
      for (int i = 0; i < num; i++) {
        NodeMetadata nodeMeta = new NodeMetadataImpl(
            "ec2", "" + roles + id, "nodeId" + id + i, 
            location, new URI("http://node" + i),
            userMetadata, ImmutableSet.<String>of(), null, null, null, null, NodeState.RUNNING, 22,
            addresses, addresses, null, loginCredentials, "hostname");
        if (i < only) {
          nodes.add(nodeMeta);
          LOG.info("{} - Node successfully started: {}", roles, nodeMeta.getId());
        } else {
          failedNodes.put(nodeMeta, new Exception("Simulated failing node"));
          LOG.info("{} - Node failing to start: {}", roles, nodeMeta.getId());
        }
      }
      if (failedNodes.size() > 0) {
        Image image = new ImageImpl("ec2", "test", "testId", location, new URI("http://node"),
            userMetadata, ImmutableSet.<String>of(), new OperatingSystem(null, null, null, null, "op", true), 
            "description", null, null, loginCredentials);
        Hardware hardware = new HardwareImpl("ec2", "test", "testId", location, new URI("http://node"),
                userMetadata, ImmutableSet.<String>of(), new ArrayList<Processor>(), 1,
                new ArrayList<Volume>(), null);
        Template template = new TemplateImpl(image, hardware, location, TemplateOptions.NONE);
        throw new RunNodesException("tag" + id, num, template, nodes, executionExceptions, failedNodes);
      }
      return nodes;
    }
  }
  
  @Test
  @SuppressWarnings("unchecked")
  public void testSubroleInvoked() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.service-name", "test-service");
    conf.addProperty("whirr.cluster-name", "test-cluster");
    conf.addProperty("whirr.instance-templates", "1 puppet:module::manifest+something-else");
    conf.addProperty("whirr.provider", "ec2");
    config.addConfiguration(conf);
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(conf);

    Set<String> nn = new HashSet<String>();
    nn.add("puppet:module::manifest"); 
    nn.add("something-else");     

    TestNodeStarterFactory nodeStarterFactory = null;
    
    ClusterActionHandlerFactory puppetHandlerFactory = mock(ClusterActionHandlerFactory.class);
    ClusterActionHandler handler = mock(ClusterActionHandler.class);
    when(puppetHandlerFactory.getRolePrefix()).thenReturn("puppet:");
    when(puppetHandlerFactory.create("module::manifest")).thenReturn(handler);
    when(handler.getRole()).thenReturn("something-else");

    Map<String, ClusterActionHandler> handlerMap = HandlerMapFactory.create(ImmutableSet.of(puppetHandlerFactory),
          ImmutableSet.of(handler));

    Function<ClusterSpec, ComputeServiceContext> getCompute = mock(Function.class);
    ComputeServiceContext serviceContext = mock(ComputeServiceContext.class);
    ComputeService computeService = mock(ComputeService.class);
    TemplateBuilder templateBuilder = mock(TemplateBuilder.class);
    Template template = mock(Template.class);

    when(getCompute.apply(clusterSpec)).thenReturn(serviceContext);
    when(serviceContext.getComputeService()).thenReturn(computeService);
    when(computeService.templateBuilder()).thenReturn(templateBuilder);
    when(templateBuilder.options((TemplateOptions) any())).thenReturn(templateBuilder);
    when(templateBuilder.build()).thenReturn(template);
    
    Map<Set<String>, Stack<Integer>> reaction = Maps.newHashMap();
    Stack<Integer> nnStack = new Stack<Integer>();
    nnStack.push(new Integer(1));
    reaction.put(nn, nnStack);
    
    nodeStarterFactory = new TestNodeStarterFactory(reaction);
    BootstrapClusterAction bootstrapper = new BootstrapClusterAction(getCompute, handlerMap, nodeStarterFactory);
    
    bootstrapper.execute(clusterSpec, null);
    
    if (nodeStarterFactory != null) {
      nodeStarterFactory.validateCompletion();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(expected = IllegalArgumentException.class)
  /** test is the same as previous (SubroleInvoked) except it knows puppet, not puppet:, as the role;
   * the colon in the role def'n is the indication it accepts subroles,
   * so this should throw IllegalArgument when we refer to puppet:module...
   */
  public void testSubroleNotSupported() throws Exception {
    CompositeConfiguration config = new CompositeConfiguration();
    if (System.getProperty("config") != null) {
      config.addConfiguration(new PropertiesConfiguration(System.getProperty("config")));
    }
    Configuration conf = new PropertiesConfiguration();
    conf.addProperty("whirr.service-name", "test-service");
    conf.addProperty("whirr.cluster-name", "test-cluster");
    conf.addProperty("whirr.instance-templates", "1 puppet:module::manifest+something-else");
    conf.addProperty("whirr.provider", "ec2");
    config.addConfiguration(conf);
    ClusterSpec clusterSpec = ClusterSpec.withTemporaryKeys(conf);

    Set<String> nn = new HashSet<String>();
    nn.add("puppet:module::manifest"); 
    nn.add("something-else");     

    TestNodeStarterFactory nodeStarterFactory = null;
    
    ClusterActionHandlerFactory puppetHandlerFactory = mock(ClusterActionHandlerFactory.class);
    ClusterActionHandler handler = mock(ClusterActionHandler.class);
    when(puppetHandlerFactory.getRolePrefix()).thenReturn("puppet");
    when(handler.getRole()).thenReturn("something-else");

    Map<String, ClusterActionHandler> handlerMap = HandlerMapFactory.create(ImmutableSet.of(puppetHandlerFactory),
          ImmutableSet.of(handler));

    Function<ClusterSpec, ComputeServiceContext> getCompute = mock(Function.class);
    ComputeServiceContext serviceContext = mock(ComputeServiceContext.class);
    ComputeService computeService = mock(ComputeService.class);
    TemplateBuilder templateBuilder = mock(TemplateBuilder.class);
    Template template = mock(Template.class);


    when(getCompute.apply(clusterSpec)).thenReturn(serviceContext);
    when(serviceContext.getComputeService()).thenReturn(computeService);
    when(computeService.templateBuilder()).thenReturn(templateBuilder);
    when(templateBuilder.options((TemplateOptions) any())).thenReturn(templateBuilder);
    when(templateBuilder.build()).thenReturn(template);
    
    Map<Set<String>, Stack<Integer>> reaction = Maps.newHashMap();
    Stack<Integer> nnStack = new Stack<Integer>();
    nnStack.push(new Integer(1));
    reaction.put(nn, nnStack);
    
    nodeStarterFactory = new TestNodeStarterFactory(reaction);
    BootstrapClusterAction bootstrapper = new BootstrapClusterAction(getCompute, handlerMap, nodeStarterFactory);
    
    bootstrapper.execute(clusterSpec, null);
    
    if (nodeStarterFactory != null) {
      nodeStarterFactory.validateCompletion();
    }
  }  
}
