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

package org.apache.whirr.service.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.whirr.service.ServiceSpec;

public class HadoopProxy {

  private ServiceSpec serviceSpec;
  private HadoopCluster cluster;
  private Process process;
  
  public HadoopProxy(ServiceSpec serviceSpec, HadoopCluster cluster) {
    this.serviceSpec = serviceSpec;
    this.cluster = cluster;
  }

  public void start() throws IOException {
    // jsch doesn't support SOCKS-based dynamic port forwarding, so we need to shell out...
    // TODO: Use static port forwarding instead?
    String identityFile = serviceSpec.getSecretKeyFile();
    String user = "ubuntu"; // TODO: get from jclouds
    String server = cluster.getNamenodePublicAddress().getHostName();
    String[] command = new String[] { "ssh",
	"-i", identityFile,
	"-o", "ConnectTimeout=10",
	"-o", "ServerAliveInterval=60",
	"-o", "StrictHostKeyChecking=no",
	"-N",
	"-D 6666",
	String.format("%s@%s", user, server)};
    ProcessBuilder processBuilder = new ProcessBuilder(command);
    process = processBuilder.start();
    
    final BufferedReader errReader = 
      new BufferedReader(new InputStreamReader(process.getErrorStream()));
    
    Thread errThread = new Thread() {
      @Override
      public void run() {
        try {
          String line = errReader.readLine();
          while((line != null) && !isInterrupted()) {
            System.err.println(line);
            line = errReader.readLine();
          }
        } catch(IOException e) {
          e.printStackTrace();
        }
      }
    };
    errThread.start();
  }
  
  public void stop() {
    process.destroy();
  }
  
}
