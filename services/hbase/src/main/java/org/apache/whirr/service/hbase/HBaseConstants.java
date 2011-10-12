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

package org.apache.whirr.service.hbase;

public final class HBaseConstants {

  public static final String KEY_TARBALL_URL = "whirr.hbase.tarball.url";

  public static final String FUNCTION_INSTALL = "install_hbase";
  public static final String FUNCTION_CONFIGURE = "configure_hbase";

  public static final String PARAM_MASTER = "-m";
  public static final String PARAM_QUORUM = "-q";
  public static final String PARAM_PORT = "-p";
  public static final String PARAM_TARBALL_URL = "-u";

  public static final String PROP_HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
  public static final String PROP_HBASE_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";

  public static final String FILE_HBASE_SITE_XML = "hbase-site.xml";
  public static final String FILE_HBASE_DEFAULT_PROPERTIES = "whirr-hbase-default.properties";

  private HBaseConstants() {
  }
}
