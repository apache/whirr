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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.security.GeneralSecurityException;
import java.security.PublicKey;
import java.security.interfaces.RSAPublicKey;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;
import org.apache.commons.ssl.PKCS8Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A convenience class for generating an RSA key pair.
 */
public class KeyPair {

  private static final Logger LOG =
    LoggerFactory.getLogger(KeyPair.class);

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
    setPermissionsTo600(privateKeyFile);

    File publicKeyFile = new File(privateKeyFile.getAbsolutePath() + ".pub");
    publicKeyFile.deleteOnExit();
    Files.write(keys.get("public").getBytes(), publicKeyFile);
    setPermissionsTo600(publicKeyFile);

    return ImmutableMap.<String, File> of("public", publicKeyFile,
              "private", privateKeyFile);
  }
  
  /**
   * Set file permissions to 600 (unix)
   */
  public static void setPermissionsTo600(File f) {
    f.setReadable(false, false);
    f.setReadable(true, true);

    f.setWritable(false, false);
    f.setWritable(true, true);

    f.setExecutable(false);
  }

  public static boolean sameKeyPair(File privateKeyFile, File publicKeyFile) throws IOException {
    try {
      PKCS8Key decodedKey = new PKCS8Key(
          new FileInputStream(privateKeyFile), null);
      PublicKey publicKey = decodedKey.getPublicKey();

      byte[] actual = encodePublicKey((RSAPublicKey) publicKey);
      byte[] expected = IOUtils.toByteArray(new FileReader(publicKeyFile));

      for(int i=0; i<actual.length; i += 1) {
        if (actual[i] != expected[i]) {
          return false;
        }
      }
      return true;
    } catch (GeneralSecurityException e) {
      LOG.error("Key pair validation failed", e);
      return false;
    }
  }

  private static byte[] encodePublicKey(RSAPublicKey key) throws IOException {
    ByteArrayOutputStream keyBlob = new ByteArrayOutputStream();
    write("ssh-rsa".getBytes(), keyBlob);
    write(key.getPublicExponent().toByteArray(), keyBlob);
    write(key.getModulus().toByteArray(), keyBlob);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    out.write("ssh-rsa ".getBytes());
    out.write(Base64.encodeBase64(keyBlob.toByteArray()));

    return out.toByteArray();
  }

  private static void write(byte[] str, OutputStream os)
  throws IOException {
    for (int shift = 24; shift >= 0; shift -= 8)
      os.write((str.length >>> shift) & 0xFF);
    os.write(str);
  }

}
