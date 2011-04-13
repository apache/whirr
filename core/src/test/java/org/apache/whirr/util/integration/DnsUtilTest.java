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

package org.apache.whirr.util.integration;

import static org.junit.Assert.*;
import static java.lang.System.out;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Enumeration;

import org.apache.whirr.util.DnsUtil;
import org.junit.Test;
import org.xbill.DNS.Address;

public class DnsUtilTest {

  @Test
  public void testResolveAddress() throws IOException {
    // test it with all interfaces
    Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
    while (en.hasMoreElements()) {
      NetworkInterface netint = (NetworkInterface) en.nextElement();
      Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
      for (InetAddress inetAddress : Collections.list(inetAddresses)) {
        if (inetAddress instanceof Inet4Address) {
          long start = System.currentTimeMillis();
          String reverse = DnsUtil.resolveAddress(inetAddress.getHostAddress());
          long end = System.currentTimeMillis();
          // we know that java.net.InetAddress's getHostName takes > 4.5s if
          // there is no reverse address assigned to it
          // DnsUtil should resolve any address in less than 5 seconds or fail
          assertTrue("DnsUtil.resolveAddress takes " + (end - start)
              + " millis, it should be shorter than five seconds",
              end - start < 5000);
          if (inetAddress.toString().substring(1).equals(reverse)) {
            out.printf(
                "InetAddress %s on interface %s does not have reverse dns name, so their reverse remains: %s\n",
                inetAddress, netint.getDisplayName(), reverse);
          } else {
            if (inetAddress.isLoopbackAddress()) {
              out.printf(
                  "InetAddress %s on loopback interface %s obtained reverse name as %s\n",
                  inetAddress, netint.getDisplayName(), reverse);
            } else {
              out.printf(
                  "InetAddress %s on interface %s has reverse dns name: %s\n",
                  inetAddress, netint.getDisplayName(), reverse);
              try {
                InetAddress checkedAddr = Address.getByName(reverse);
                assertEquals(inetAddress, checkedAddr);
              } catch (UnknownHostException uhex) {
                fail("InetAddress " + inetAddress + " on interface "
                    + netint.getDisplayName() + " got " + reverse
                    + " reverse dns name which in return is an unknown host!");
              }
            }
          }
        }
      }
    }
  }

}
