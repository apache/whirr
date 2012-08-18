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

package org.apache.whirr.net;

import static org.xbill.DNS.Message.newQuery;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.DClass;
import org.xbill.DNS.ExtendedResolver;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import org.xbill.DNS.Resolver;
import org.xbill.DNS.ReverseMap;
import org.xbill.DNS.Section;
import org.xbill.DNS.Type;

/**
 * Fast DNS resolver
 */
public class FastDnsResolver implements DnsResolver {

  private static final Logger LOG = LoggerFactory
      .getLogger(FastDnsResolver.class);

  private int timeoutInSeconds;

  public FastDnsResolver() {
    this(5);  // default to 5 seconds
  }

  public FastDnsResolver(int timeoutInSeconds) {
    this.timeoutInSeconds = timeoutInSeconds;
  }

  /**
   * Resolve the reverse dns name for the given IP address
   * 
   * @param hostIp
   *      host IP address
   * @return
   *      the resolved DNS name or in some cases the IP address as a string
   */
  @Override
  public String apply(String hostIp) {
    try {
      Resolver resolver = new ExtendedResolver();
      resolver.setTimeout(timeoutInSeconds);
      resolver.setTCP(true);

      Name name = ReverseMap.fromAddress(hostIp);
      Record record = Record.newRecord(name, Type.PTR, DClass.IN);
      Message response = resolver.send(newQuery(record));

      Record[] answers = response.getSectionArray(Section.ANSWER);
      if (answers.length == 0) {
        LOG.warn("no answer to DNS resolution attempt for "+hostIp+"; using fallback");
        return fallback(hostIp);
      } else {
        String reverseAddress = answers[0].rdataToString();
        return reverseAddress.endsWith(".") ? reverseAddress.substring(0, reverseAddress.length() - 1) : reverseAddress;
      }
    } catch(SocketTimeoutException e) {
      return hostIp;  /* same response as standard Java on timeout */

    } catch(IOException e) {
      // suggests eg firewall block DNS lookup or similar
      LOG.warn("error in DNS resolution attempt for "+hostIp+" ("+e+"); using fallback");
      return fallback(hostIp);
    }
  }

  /**
   * Use standard Java for reverse DNS name resolution. This also
   * reads /etc/hosts but it may take longer
   *
   * @param hostIp
   *      host IP address
   * @return
   *      the fully qualified domain name for this IP address, or if the operation
   *      is not allowed by the security check, the textual representation of the IP address.
   */
  private String fallback(String hostIp) {
    return new InetSocketAddress(hostIp, 0).getAddress().getCanonicalHostName();
  }

}

