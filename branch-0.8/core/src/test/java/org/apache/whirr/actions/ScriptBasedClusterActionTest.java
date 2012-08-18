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

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.jclouds.scriptbuilder.domain.Statements.exec;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import org.apache.whirr.Cluster;
import org.apache.whirr.ClusterController;
import org.apache.whirr.ClusterSpec;
import org.apache.whirr.HandlerMapFactory;
import org.apache.whirr.InstanceTemplate;
import org.apache.whirr.service.ClusterActionEvent;
import org.apache.whirr.service.ClusterActionHandler;
import org.apache.whirr.service.ClusterActionHandlerSupport;
import org.apache.whirr.service.ComputeCache;
import org.apache.whirr.service.DryRunModule.DryRun;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.events.StatementOnNode;
import org.jclouds.scriptbuilder.domain.OsFamily;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public abstract class ScriptBasedClusterActionTest<T extends ScriptBasedClusterAction> {

  public static class Noop1ClusterActionHandler extends
      ClusterActionHandlerSupport {

    @Override
    public String getRole() {
      return "noop1";
    }

    @Override
    public void beforeConfigure(ClusterActionEvent event) {
      addStatement(event, exec("echo noop1-configure"));
    }

    @Override
    public void beforeStart(ClusterActionEvent event) {
      addStatement(event, exec("echo noop1-start"));
    }

    @Override
    public void beforeStop(ClusterActionEvent event) {
      addStatement(event, exec("echo noop1-stop"));
    }

    @Override
    public void beforeCleanup(ClusterActionEvent event) {
      addStatement(event, exec("echo noop1-cleanup"));
    }

    @Override
    public void beforeDestroy(ClusterActionEvent event) {
      addStatement(event, exec("echo noop1-destroy"));
    }
  }

  private final static HandlerMapFactory HANDLER_MAP_FACTORY = new HandlerMapFactory();
  private final static LoadingCache<String, ClusterActionHandler> HANDLERMAP = HANDLER_MAP_FACTORY.create();
  private final static Set<String> EMPTYSET = ImmutableSet.of();

  private ClusterSpec clusterSpec;
  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    clusterSpec = ClusterSpec.withTemporaryKeys();

    clusterSpec.setClusterName("test-cluster-for-script-exection");
    clusterSpec.setProvider("stub");
    clusterSpec.setIdentity("dummy");
    clusterSpec.setStateStore("none");

    clusterSpec.setInstanceTemplates(ImmutableList.of(
        newInstanceTemplate("noop"),
        newInstanceTemplate("noop", "noop1", "noop2"),
        newInstanceTemplate("noop1", "noop3")
    ));

    ClusterController controller = new ClusterController();
    cluster = controller.launchCluster(clusterSpec);
  }

  private InstanceTemplate newInstanceTemplate(String... roles) {
    return InstanceTemplate.builder().numberOfInstance(1).roles(roles).build();
  }

  @Test
  public void testActionIsExecutedOnAllRelevantNodes() throws Exception {
    T action = newClusterActionInstance(EMPTYSET, EMPTYSET);
    DryRun dryRun = getDryRunForAction(action).reset();
    
    action.execute(clusterSpec, cluster);
    
    List<StatementOnNode> executions = dryRun.getTotallyOrderedExecutions();

    // only 2 out of 3 because one instance has only noop
    assertThat(executions.size(), is(2));
  }

  public DryRun getDryRunForAction(T action) {
    return action.getCompute().apply(clusterSpec).utils().injector().getInstance(DryRun.class);
  }

  @Test
  public void testFilterScriptExecutionByRole() throws Exception {
    String instanceId = getInstaceForRole(cluster, "noop2").getId();
    T action = newClusterActionInstance(ImmutableSet.of("noop2"), EMPTYSET);
    DryRun dryRun = getDryRunForAction(action).reset();
    
    action.execute(clusterSpec, cluster);

    List<StatementOnNode> executions = dryRun.getTotallyOrderedExecutions();

    assertThat(executions.size(), is(1));
    assertHasRole(executions.get(0), cluster, "noop2");
    assertEquals(executions.get(0).getNode().getId(), instanceId);

    assertAnyStatementContains(dryRun, "noop2-" + getActionName());
    assertNoStatementContains(dryRun, "noop1-" + getActionName(), "noop3-" + getActionName());
  }

  @Test
  public void testFilterScriptExecutionByInstanceId() throws Exception {
    String instanceId = getInstaceForRole(cluster, "noop3").getId();
    T action = newClusterActionInstance(EMPTYSET, newHashSet(instanceId));
    DryRun dryRun = getDryRunForAction(action).reset();

    action.execute(clusterSpec, cluster);

    List<StatementOnNode> executions = dryRun.getTotallyOrderedExecutions();
    
    assertThat(executions.size(), is(1));
    assertHasRole(executions.get(0), cluster, "noop3");
    assertEquals(executions.get(0).getNode().getId(), instanceId);

    assertAnyStatementContains(dryRun, "noop1-" + getActionName(), "noop3-" + getActionName());
  }

  @Test
  public void testFilterScriptExecutionByRoleAndInstanceId() throws Exception {
    String instanceId = getInstaceForRole(cluster, "noop1").getId();
    T action = newClusterActionInstance(newHashSet("noop1"), newHashSet(instanceId));
    DryRun dryRun = getDryRunForAction(action).reset();

    action.execute(clusterSpec, cluster);

    List<StatementOnNode> executions = dryRun.getTotallyOrderedExecutions();
    
    assertThat(executions.size(), is(1));
    assertHasRole(executions.get(0), cluster, "noop1");
    assertEquals(executions.get(0).getNode().getId(), instanceId);

    assertAnyStatementContains(dryRun, "noop1-" + getActionName());
    assertNoStatementContains(dryRun, "noop2-" + getActionName(), "noop3-" + getActionName());
  }

  @Test
  public void testNoScriptExecutionsForNoop() throws Exception {
    T action = newClusterActionInstance(ImmutableSet.of("noop"), EMPTYSET);
    DryRun dryRun = getDryRunForAction(action).reset();
    
    action.execute(clusterSpec, cluster);

    List<StatementOnNode> executions = dryRun.getTotallyOrderedExecutions();
    
    // empty because noop does not emit any statements
    assertThat(executions.size(), is(0));
  }

  private void assertHasRole(final StatementOnNode node, Cluster cluster, String... roles) {
    Cluster.Instance instance = getInstanceForNode(node, cluster);
    for (String role : roles) {
      assertTrue(instance.getRoles().contains(role));
    }
  }

  private void assertAnyStatementContains(DryRun dryRun, String... values) {
    Set<String> toCheck = newHashSet(values);
    for (StatementOnNode node : dryRun.getTotallyOrderedExecutions()) {
      String statement = node.getStatement().render(OsFamily.UNIX);
      for (String term : ImmutableSet.copyOf(toCheck)) {
        if (statement.contains(term)) {
          toCheck.remove(term);
        }
      }
    }
    assertTrue("Unable to find the following terms in any statement: " +
        toCheck.toString(), toCheck.size() == 0);
  }

  private void assertNoStatementContains(DryRun dryRun, String... values) {
    Set<String> foundTerms = newHashSet();
    for (StatementOnNode node : dryRun.getTotallyOrderedExecutions()) {
      String statement = node.getStatement().render(OsFamily.UNIX);
      for (String term : values) {
        if (statement.contains(term)) {
          foundTerms.add(term);
        }
      }
    }
    assertTrue("Some terms are present in statements: " +
        foundTerms, foundTerms.size() == 0);
  }

  private Cluster.Instance getInstaceForRole(Cluster cluster, final String role) {
    return Iterables.find(cluster.getInstances(),
        new Predicate<Cluster.Instance>() {
          @Override
          public boolean apply(Cluster.Instance instance) {
            return instance.getRoles().contains(role);
          }
        });
  }

  private Cluster.Instance getInstanceForNode(final StatementOnNode node, Cluster cluster) {
    return Iterables.find(cluster.getInstances(),
        new Predicate<Cluster.Instance>() {
          @Override
          public boolean apply(Cluster.Instance input) {
            return input.getId().equals(node.getNode().getId());
          }
        });
  }

  private T newClusterActionInstance(Set<String> targetRoles, Set<String> targetInstanceIds) {
    return newClusterActionInstance(ComputeCache.INSTANCE, HANDLERMAP, targetRoles, targetInstanceIds);
  }

  public abstract T newClusterActionInstance(
      Function<ClusterSpec, ComputeServiceContext> getCompute,
      LoadingCache<String, ClusterActionHandler> handlerMap,
      Set<String> targetRoles,
      Set<String> targetInstanceIds
  );
  
  public abstract String getActionName();
}
