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

import static org.apache.whirr.util.KeyPair.sameKeyPair;
import static org.apache.whirr.util.KeyPair.generate;
import static org.apache.whirr.util.KeyPair.generateTemporaryFiles;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Test;

import com.jcraft.jsch.JSchException;

public class KeyPairTest {

  @Test
  public void testNotFromSamePair() throws JSchException, IOException {
    Map<String, File> keys = generateTemporaryFiles();
    assertThat(sameKeyPair(keys.get("private"), keys.get("public")), is(true));

    Map<String, File> other = generateTemporaryFiles();
    assertThat(sameKeyPair(other.get("private"), other.get("public")), is(true));

    assertThat(sameKeyPair(keys.get("private"), other.get("public")), is(false));
    assertThat(sameKeyPair(other.get("private"), keys.get("public")), is(false));
  }

  @Test
  public void testGenerate() throws JSchException {
    Map<String, String> pair = generate();
    assertThat(pair.get("public"),
        containsString("ssh-rsa "));
    assertThat(pair.get("private"),
          containsString("-----BEGIN RSA PRIVATE KEY-----\n"));
  }
  
}
