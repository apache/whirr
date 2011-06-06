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

package org.apache.whirr.service.jclouds;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import java.util.List;

import javax.inject.Singleton;

import org.jclouds.domain.Credentials;
import org.jclouds.ec2.compute.strategy.EC2PopulateDefaultLoginCredentialsForImageStrategy;

@Singleton
// patch until jclouds http://code.google.com/p/jclouds/issues/detail?id=441
public class TakeLoginCredentialsFromWhirrProperties extends
    EC2PopulateDefaultLoginCredentialsForImageStrategy {

  @Override
  public Credentials execute(Object resourceToAuthenticate) {
    if (System.getProperties().containsKey("whirr.login-user") &&
       !"".equals(System.getProperty("whirr.login-user").trim())) {
      List<String> creds = Lists.newArrayList(Splitter.on(':').split(System.getProperty("whirr.login-user")));
      if (creds.size() == 2)
         return new Credentials(creds.get(0), creds.get(1));
      return new Credentials(creds.get(0), null);
    } else {
       return super.execute(resourceToAuthenticate);
    }
  }
}
