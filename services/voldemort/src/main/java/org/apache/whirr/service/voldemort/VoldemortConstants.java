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

package org.apache.whirr.service.voldemort;

public final class VoldemortConstants {

  public static final String KEY_TARBALL_URL = "whirr.voldemort.tarball.url";

  public static final String KEY_CONF_URL = "whirr.voldemort.conf.url";

  public static final String KEY_PARTITIONS_PER_NODE = "whirr.voldemort.partitions";

  public static final String FUNCTION_INSTALL = "install_voldemort";

  public static final String FUNCTION_CONFIGURE = "configure_voldemort";

  public static final String PARAM_TARBALL_URL = "-u";

  public static final String PARAM_CONF_URL = "-f";

  public static final String PARAM_PARTITIONS_PER_NODE = "-p";

  public static final String ROLE = "voldemort";

  public static final int CLIENT_PORT = 6666;

  public static final int ADMIN_PORT = 6667;

  public static final int HTTP_PORT = 8081;

  private VoldemortConstants() {
  }

}
