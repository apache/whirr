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

import com.google.common.base.Charsets;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;

public class ServiceSpec {
  
  private String provider;
  private String account;
  private String key;
  private String clusterName;
  private String secretKeyFile;
  private String amiOwners;
  
  public String getProvider() {
    return provider;
  }
  public String getAccount() {
    return account;
  }
  public String getKey() {
    return key;
  }
  public String getClusterName() {
    return clusterName;
  }
  public String getSecretKeyFile() {
    return secretKeyFile;
  }
  public String getAmiOwners() {
    return amiOwners;
  }
  public void setProvider(String provider) {
    this.provider = provider;
  }
  public void setAccount(String account) {
    this.account = account;
  }
  public void setKey(String key) {
    this.key = key;
  }
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }
  public void setSecretKeyFile(String secretKeyFile) {
    this.secretKeyFile = secretKeyFile;
  }
  public void setAmiOwners(String amiOwners) {
    this.amiOwners = amiOwners;
  }
  
  //
  public String readPrivateKey() throws IOException {
    return Files.toString(new File(getSecretKeyFile()), Charsets.UTF_8);
  }
    
  public String readPublicKey() throws IOException {
    return Files.toString(new File(getSecretKeyFile() + ".pub"), Charsets.UTF_8);
  }
    
}
