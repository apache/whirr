/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.whirr.service.puppet;

import static junit.framework.Assert.assertEquals;

import org.junit.Test;

public class ManifestTest {

  private static final String NGINX_PUPPET = "class { 'nginx':\n}";
  private static final String POSTGRES_PUPPET = "class { 'postgresql::server':\n}";
  private static final String NTP_PUPPET = "class { 'ntp':\n  servers => \"10.0.0.1\",\n}";
  private static final String NTP_PUPPET_HIERA = "\nntp::servers: \"10.0.0.1\"";
  private static final String POSTGRES_PUPPET_NAME = "postgresql::server";

  // private static final String PUPPET_COMMAND =
  // "echo 'class { 'ntp::client':\n  servers => [\"10.0.0.1\"],\n}' >> /tmp/test.pp\nsudo -i puppet apply /tmp/test.pp\n";
  
  @Test
  public void testPuppetConversion() {

   Manifest nginx = new Manifest("nginx", null);

   assertEquals("Puppet representation is incorrect.", NGINX_PUPPET,
      nginx.toString());
  }

  @Test
  public void testPuppetConversionWithSpecificClass() {

   Manifest postgresql = new Manifest("postgresql", "server");

   assertEquals("PUPPET representation is incorrect.", POSTGRES_PUPPET,
      postgresql.toString());

  }

  @Test
  public void testPuppetConversionWithAttribs() {

   Manifest ntp = new Manifest("ntp");
   ntp.attribs.put("servers", "\"10.0.0.1\"");

   assertEquals("Puppet representation is incorrect.", NTP_PUPPET,
      ntp.toString());

  }

  @Test
  public void testPuppetConversionWithAttribsHiera() {

    Manifest ntp = new Manifest("ntp");
    ntp.attribs.put("servers", "\"10.0.0.1\"");

    assertEquals("Puppet/Hiera representation is incorrect.", NTP_PUPPET_HIERA,
       ntp.getHiera());
  }

  @Test
  public void testPuppetConversionHiera() {

    Manifest postgress = new Manifest("postgresql", "server");

    assertEquals("Puppet/Name representation is incorrect.", POSTGRES_PUPPET_NAME,
              postgress.getName());
    }
}

