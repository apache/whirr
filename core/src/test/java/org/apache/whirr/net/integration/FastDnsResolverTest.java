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

package org.apache.whirr.net.integration;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.whirr.net.DnsResolver;
import org.apache.whirr.net.FastDnsResolver;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.Address;

public class FastDnsResolverTest {

  private static final Logger LOG = LoggerFactory.getLogger(FastDnsResolverTest.class);

  protected DnsResolver getDnsResolver() {
    return new FastDnsResolver();
  }

  /**
   * Try the reverse DNS name resolver on all IPv4 interfaces
   *
   * @throws IOException
   */
  @Test
  public void testResolveAddress() throws IOException {
    long start, end;
    Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();

    while (en.hasMoreElements()) {
      NetworkInterface netint = en.nextElement();
      Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();

      for (InetAddress inetAddress : Collections.list(inetAddresses)) {
        if (inetAddress instanceof Inet4Address) {

          /* We know that java.net.InetAddress's getHostName takes > 4.5s if
            there is no reverse address assigned to it. FastDnsResolver should
            resolve any address in less than 5 seconds or return the IP */

          start = currentTimeMillis();
          String reverse = getDnsResolver().apply(inetAddress.getHostAddress());
          end = currentTimeMillis();

          assertTrue("resolveAddress takes " + (end - start) + " millis, it should be " +
            "shorter than 5 seconds", (end - start) < 5500);

          checkResponse(reverse, netint, inetAddress);
        }
      }
    }
  }

  private void checkResponse(String reverse, NetworkInterface networkInterface, InetAddress inetAddress) {
    if (inetAddress.toString().substring(1).equals(reverse)) {
      LOG.info(String.format("InetAddress %s on interface %s does not have reverse dns name, " +
        "so their reverse remains: %s", inetAddress, networkInterface.getDisplayName(), reverse));

    } else {
      if (inetAddress.isLoopbackAddress()) {
        LOG.info(String.format("InetAddress %s on loopback interface %s obtained reverse name as %s",
          inetAddress, networkInterface.getDisplayName(), reverse));

      } else {
        LOG.info(String.format("InetAddress %s on interface %s has reverse dns name: %s\n",
          inetAddress, networkInterface.getDisplayName(), reverse));

        try {
          InetAddress checkedAddr = Address.getByName(reverse);
          assertEquals(inetAddress, checkedAddr);

        } catch (UnknownHostException uhex) {
          fail("InetAddress " + inetAddress + " on interface " + networkInterface.getDisplayName() +
            " got " + reverse + " reverse dns name which in return is an unknown host!");
        }
      }
    }
  }

}
