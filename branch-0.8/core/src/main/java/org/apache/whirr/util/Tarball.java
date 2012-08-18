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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Tarball utility.
 */
public class Tarball {

  /**
   * Creates a tarball from the source directory and writes it into the target directory.
   *
   * @param sourceDirectory directory whose files will be added to the tarball
   * @param targetName      directory where tarball will be written to
   * @throws IOException when an exception occurs on creating the tarball
   */
  public static void createFromDirectory(String sourceDirectory, String targetName) throws IOException {
    FileOutputStream fileOutputStream = null;
    BufferedOutputStream bufferedOutputStream = null;
    GzipCompressorOutputStream gzipOutputStream = null;
    TarArchiveOutputStream tarArchiveOutputStream = null;

    try {
      fileOutputStream = new FileOutputStream(new File(targetName));
      bufferedOutputStream = new BufferedOutputStream(fileOutputStream);
      gzipOutputStream = new GzipCompressorOutputStream(bufferedOutputStream);
      tarArchiveOutputStream = new TarArchiveOutputStream(gzipOutputStream);

      addFilesInDirectory(tarArchiveOutputStream, sourceDirectory);
    } finally {
      if (tarArchiveOutputStream != null) {
        tarArchiveOutputStream.finish();
      }
      if (tarArchiveOutputStream != null) {
        tarArchiveOutputStream.close();
      }
      if (gzipOutputStream != null) {
        gzipOutputStream.close();
      }
      if (bufferedOutputStream != null) {
        bufferedOutputStream.close();
      }
      if (fileOutputStream != null) {
        fileOutputStream.close();
      }
    }
  }

  private static void addFilesInDirectory(TarArchiveOutputStream tarOutputStream, String path) throws IOException {
    File file = new File(path);
    File[] children = file.listFiles();

    if (children != null) {
      for (File child : children) {
        addFile(tarOutputStream, child.getAbsolutePath(), "/");
      }
    }
  }

  private static void addFile(TarArchiveOutputStream tarOutputStream, String path, String base) throws IOException {
    File file = new File(path);
    String entryName = base + file.getName();
    TarArchiveEntry tarEntry = new TarArchiveEntry(file, entryName);

    tarOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    tarOutputStream.putArchiveEntry(tarEntry);

    if (file.isFile()) {
      IOUtils.copy(new FileInputStream(file), tarOutputStream);
      tarOutputStream.closeArchiveEntry();
    } else {
      tarOutputStream.closeArchiveEntry();
      File[] children = file.listFiles();
      if (children != null) {
        for (File child : children) {
          addFile(tarOutputStream, child.getAbsolutePath(), entryName + "/");
        }
      }
    }
  }
}
