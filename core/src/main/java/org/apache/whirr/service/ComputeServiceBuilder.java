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

import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;

import java.io.IOException;
import java.util.Set;

import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.ComputeServiceContextFactory;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.ssh.jsch.config.JschSshClientModule;

public class ComputeServiceBuilder {

  public static ComputeService build(ServiceSpec spec) throws IOException {
    Set<AbstractModule> wiring = ImmutableSet.of(new JschSshClientModule(),
      new Log4JLoggingModule());

    ComputeServiceContext context = new ComputeServiceContextFactory()
      .createContext(spec.getProvider(), spec.getAccount(), spec.getKey(),
        wiring);

    return context.getComputeService();
  }
}
