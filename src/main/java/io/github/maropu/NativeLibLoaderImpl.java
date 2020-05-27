/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.maropu;

import java.io.*;
import java.util.UUID;

import org.xerial.snappy.OSInfo;

/**
 * Common functionality for native library loaders.
 */
public class NativeLibLoaderImpl {

  private static final String OS = OSInfo.getOSName();
  private static final String ARCH = OSInfo.getArchName();

  private String libraryName;

  public NativeLibLoaderImpl(String name) {
    this.libraryName = name;
  }

  private static String getVersion() {
    // TODO: Get the version from the root `pom.xml`
    return "0.1.0";
  }

  // Loads the OS-dependent native library inside the jar file
  public File loadNativeLibFromJar() {
    checkIfPlatformSupported();

    String nativeLibName = System.mapLibraryName(libraryName);
    String nativeLibPath = String.format("/lib/%s/%s", OS, ARCH);

    // We assume that we have already checked if this platform is supported in advance
    assert(hasResource(nativeLibPath + "/" + nativeLibName));

    // Attaches UUID to the extracted native library to ensure multiple class loaders
    // can read it multiple times.
    String outputName = String.format("%s-%s-%s", getVersion(), UUID.randomUUID().toString(), nativeLibName);
    File extractedFile = extractFileFromJar(nativeLibPath, nativeLibName, outputName);

    // Sets executable (x) flag to enable Java to load the library
    extractedFile.setReadable(true);
    extractedFile.setWritable(true, true);
    extractedFile.setExecutable(true);

    return extractedFile;
  }

  public static void checkIfPlatformSupported() {
    // TODO: Supports the Linux platform
    // if (!((OS.equals("Linux") || OS.equals("Mac")) && ARCH.equals("x86_64"))) {
    if (!(OS.equals("Mac") && ARCH.equals("x86_64"))) {
      final String errMsg = String.format(
        "Unsupported platform: os.name=%s and os.arch=%s", OS, ARCH);
      throw new UnsupportedOperationException(errMsg);
    }
  }

  private static boolean contentsEquals(InputStream in1, InputStream in2) throws IOException {
    if (!(in1 instanceof BufferedInputStream)) {
      in1 = new BufferedInputStream(in1);
    }
    if (!(in2 instanceof BufferedInputStream)) {
      in2 = new BufferedInputStream(in2);
    }
    int ch = in1.read();
    while (ch != -1) {
      int ch2 = in2.read();
      if (ch != ch2) {
        return false;
      }
      ch = in1.read();
    }
    int ch2 = in2.read();
    return ch2 == -1;
  }

  public static File extractFileFromJar(String srcDirInJar, String fileName, File dstDir) {
    return extractFileFromJar(srcDirInJar, fileName, dstDir, fileName);
  }

  public static File extractFileFromJar(String srcDirInJar, String fileName, String outputName) {
    File tmpDir = Utils.createTempDir();
    return extractFileFromJar(srcDirInJar, fileName, tmpDir, outputName);
  }

  private static File extractFileFromJar(
      String srcDirInJar,
      String fileName,
      File dstDir,
      String outputName) {

    String filePathInJar = srcDirInJar + "/" + fileName;
    File extractedFile = new File(dstDir, outputName);

    try {
      // Extracts the file into the target directory
      InputStream reader = null;
      FileOutputStream writer = null;
      try {
        reader = NativeLibLoaderImpl.class.getResourceAsStream(filePathInJar);
        try {
          writer = new FileOutputStream(extractedFile);

          byte[] buffer = new byte[8192];
          int bytesRead = 0;
          while ((bytesRead = reader.read(buffer)) != -1) {
            writer.write(buffer, 0, bytesRead);
          }
        } finally {
          if (writer != null) {
            writer.close();
          }
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }

      // Checks whether the contents are properly copied from the resource directory
      InputStream jarFileIn = null;
      InputStream extractedFileIn = null;
      try {
        jarFileIn = NativeLibLoaderImpl.class.getResourceAsStream(filePathInJar);
        extractedFileIn = new FileInputStream(extractedFile);

        if (!contentsEquals(jarFileIn, extractedFileIn)) {
          String errMsg = String.format("Can't extract the file: %s", filePathInJar);
          throw new RuntimeException(errMsg);

        }
      } finally {
        if (jarFileIn != null) {
          jarFileIn.close();
        }
        if (extractedFileIn != null) {
          extractedFileIn.close();
        }
      }
      return new File(dstDir, outputName);
    } catch (IOException e) {
      e.printStackTrace(System.err);
      return null;
    }
  }

  private static boolean hasResource(String path) {
    return NativeLibLoaderImpl.class.getResource(path) != null;
  }
}
