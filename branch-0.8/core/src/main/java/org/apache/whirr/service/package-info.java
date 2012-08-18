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
 * 
 */ 
/** 
 * <p>
 * The Whirr Service API.
 * </p> 
 * 
 * <h3>Terminology</h3>
 * 
 * <p>
 * A <i>service</i> is an instance of a coherent system running on one or more
 * machines.
 * </p>
 * <p>
 * A <i>role</i> is a part of a service running on a single machine. A role
 * typically corresponds to a single process or daemon, although this is not
 * required.
 * </p>
 * <p>
 * An <i>instance template</i> ({@link org.apache.whirr.InstanceTemplate}) is a specification of the role sets and
 * cardinalities that make up a cluster. For example,
 * <tt>1 role-a+role-b,4 role-c</tt>
 * specifies a cluster in which one node is in roles <tt>role-a</tt> and
 * <tt>role-b</tt>, and four nodes are in role <tt>role-c</tt>.
 * </p>
 * <p>
 * A <i>cluster action</i> ({@link org.apache.whirr.ClusterAction}) is an action that is performed on a set of machines
 * in a cluster. Examples of cluster actions include 'bootstrap' and 'configure'.
 * </p>
 * 
 * <h3>Orchestration</h3>
 * 
 * <p>
 * You can launch or destroy clusters using an instance of
 * {@link org.apache.whirr.ClusterController}.
 * </p>
 * 
 * <p>
 * Whirr {@link org.apache.whirr.ClusterController#launchCluster(ClusterSpec) launches a cluster} by running the bootstrap action, followed by the
 * configure action. For each of these actions Whirr follows these rules:
 * </p>
 * 
 * <ol>
 * <li>Instance templates are acted on independently.</li>
 * <li>For each instance template Whirr will call the cluster action handlers
 * for each role in the template.</li>
 * </ol>
 * 
 * <p>
 * The first rule implies that you can't rely on one template being processed
 * first. In fact, Whirr will process them all in parallel. So to transfer
 * information from one role to another (in a different template) you should use
 * different actions. For example, use the configure phase to get information
 * about another role that was set in its bootstrap phase.
 * </p>
 * 
 * <p>
 * The second rule implies that a cluster action handler for a given role
 * will be run more than once if it appears in more than one template.
 * </p>
 * 
 * <p>
 * A cluster is {@link org.apache.whirr.ClusterController#destroyCluster(ClusterSpec) destroyed} by running the destroy action.
 * </p>
 * 
 * <h3>Writing a New Service</h3>
 * 
 * <p>
 * For each role in a service you must write a
 * {@link org.apache.whirr.service.ClusterActionHandler}, which allows you to
 * run code at various points in the lifecycle. For example, you can specify a
 * script that must be run on bootstrap, and another at configuration time.
 * </p>
 * 
 * <p>
 * Roles for a service are discovered using Java's service-provider loading
 * facility defined by {@link java.util.ServiceLoader}. It's very easy to
 * register your service. Simply create a file with the following path (assuming
 * a Maven directory structure):
 * </p>
 * 
 * <p>
 * <i>src/main/resources/META-INF/services/org.apache.whirr.service.ClusterActionHandler</i>
 * </p>
 * 
 * <p>
 * Then for each {@link org.apache.whirr.service.ClusterActionHandler} 
 * implementation add its fully qualified name as a line in the file:
 * </p>
 * 
 * <pre>
 * org.example.MyClusterActionHandlerForRoleA
 * org.example.MyClusterActionHandlerForRoleB
 * </pre>
 * 
 * <p>
 * If you service is not a part of Whirr, then you can install it by first
 * installing Whirr, then dropping the JAR file for your service into
 * Whirr's <i>lib</i> directory.
 * </p>
 */
package org.apache.whirr.service;
