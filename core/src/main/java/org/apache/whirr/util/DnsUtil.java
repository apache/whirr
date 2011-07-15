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

package org.apache.whirr.util;

import java.io.IOException;
import java.net.InetSocketAddress;

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
 * Utility functions for DNS.
 */
public class DnsUtil {

  /**
   * resolve the reverse dns name for the given IP address
   * 
   * @param hostIp
   * @return The resolved DNS name.
   * @throws IOException
   */
  public static String resolveAddress(String hostIp) throws IOException {
    Resolver res = new ExtendedResolver();
    res.setTimeout(5); // seconds

    Name name = ReverseMap.fromAddress(hostIp);
    int type = Type.PTR;
    int dclass = DClass.IN;
    Record rec = Record.newRecord(name, type, dclass);
    Message query = Message.newQuery(rec);
    Message response = res.send(query);

    Record[] answers = response.getSectionArray(Section.ANSWER);
    if (answers.length == 0) {
      // Fall back to standard Java: in contrast to dnsjava, this also reads /etc/hosts
      return new InetSocketAddress(hostIp, 0).getAddress().getCanonicalHostName();
    } else {
      String revaddr = answers[0].rdataToString();
      return revaddr.endsWith(".") ? revaddr.substring(0, revaddr.length() - 1) : revaddr;
    }
  }
}
