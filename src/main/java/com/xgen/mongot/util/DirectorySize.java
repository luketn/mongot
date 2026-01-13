package com.xgen.mongot.util;

import com.google.errorprone.annotations.Var;
import java.io.File;
import java.nio.file.Files;
import java.util.Objects;

/**
 * This class represents code retained and modified from Apache commons-io version 2.11.0. We need
 * to upgrade the library due to a CVE, but the new version of FileUtils.sizeOfDirectory throws
 * exceptions during the course of normal operations. By using the old code, we can retain the
 * exception-less behavior we see below.
 *
 * @see <a
 *     href="https://github.com/apache/commons-io/blob/rel/commons-io-2.11.0/src/main/java/org/apache/commons/io/FileUtils.java#L2888-L2890">github
 *     source for commons-io 2.11</a>
 */
public abstract class DirectorySize {
  /**
   * calculates the size of the given directory in bytes
   *
   * @param directory the target directory
   * @return size in bytes
   * @throws NullPointerException if directory is null
   * @throws IllegalArgumentException if directory does not exist or is not a directory
   */
  public static long of(File directory) {
    return sizeOfDirectory0(requireDirectoryExists(directory));
  }

  private static File requireDirectoryExists(File directory) {
    Objects.requireNonNull(directory, "directory");
    Check.checkArg(
        directory.exists(),
        "File system element for parameter 'directory' does not exist: '%s'",
        directory);
    Check.checkArg(
        directory.isDirectory(), "Parameter 'directory' is not a directory: '%s'", directory);
    return directory;
  }

  /** Gets the size of a directory. */
  private static long sizeOfDirectory0(File directory) {
    File[] files = directory.listFiles();
    if (files == null) { // null if security restricted
      return 0L;
    }
    @Var long size = 0;

    for (File file : files) {
      if (!isSymlink(file)) {
        size += sizeOf0(file);
        if (size < 0) {
          break;
        }
      }
    }

    return size;
  }

  /**
   * Gets the size of a file.
   *
   * @param file the file to check.
   * @return the size of the file.
   * @throws NullPointerException if the file is {@code null}.
   */
  private static long sizeOf0(File file) {
    Objects.requireNonNull(file, "file");
    if (file.isDirectory()) {
      return sizeOfDirectory0(file);
    }
    return file.length(); // will be 0 if file does not exist
  }

  private static boolean isSymlink(File file) {
    return file != null && Files.isSymbolicLink(file.toPath());
  }

  private DirectorySize() {
    // utility class: do not inherit or instantiate
  }
}
