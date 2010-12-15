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

package org.apache.whirr.ssh;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

/**
 * A convenience class for generating an RSA key pair.
 */
public class KeyPair {

  public static Map<String, String> generate() throws JSchException {
      return generate(null);
  }

  /**
   * return a "public" -> rsa public key, "private" -> its corresponding
   *   private key
   */
  public static Map<String,String> generate(String passPhrase)
  throws JSchException {
    com.jcraft.jsch.KeyPair pair = com.jcraft.jsch.KeyPair.genKeyPair(
        new JSch(),  com.jcraft.jsch.KeyPair.RSA);
    if (passPhrase != null) {
      pair.setPassphrase(passPhrase);
    }

    ByteArrayOutputStream publicKeyOut = new ByteArrayOutputStream();
    ByteArrayOutputStream privateKeyOut = new ByteArrayOutputStream();

    pair.writePublicKey(publicKeyOut, "whirr");
    pair.writePrivateKey(privateKeyOut);

    String publicKey = new String(publicKeyOut.toByteArray());
    String privateKey = new String(privateKeyOut.toByteArray());

    return ImmutableMap.<String, String> of("public", publicKey,
        "private", privateKey);
  }

  public static Map<String, File> generateTemporaryFiles()
  throws JSchException, IOException {
    return generateTemporaryFiles(null); 
  }

  public static Map<String, File> generateTemporaryFiles(String passPhrase)
  throws JSchException, IOException {
    Map<String, String> keys = KeyPair.generate(passPhrase);

    File privateKeyFile = File.createTempFile("private", "key");
    privateKeyFile.deleteOnExit();
    Files.write(keys.get("private").getBytes(), privateKeyFile);

    File publicKeyFile = new File(privateKeyFile.getAbsolutePath() + ".pub");
    publicKeyFile.deleteOnExit();
    Files.write(keys.get("public").getBytes(), publicKeyFile);

    return ImmutableMap.<String, File> of("public", publicKeyFile,
              "private", privateKeyFile);
  }
}
