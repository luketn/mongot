package com.xgen.mongot.util;

import com.xgen.testing.TestUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileUtilsTest {
  @Test
  public void testMkdirIfNotExist() throws IOException {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();
    Path dir = getChildFilePath(root.toPath(), "dir");
    FileUtils.mkdirIfNotExist(dir);
    Assert.assertTrue(dir.toFile().exists());
    FileUtils.mkdirIfNotExist(dir); // should not throw anything
  }

  @Test
  public void testAtomicallyReplaceFileDoesNotYetExist() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();

    String fileName = "file.txt";
    Path filePath = getChildFilePath(root.toPath(), fileName);
    String contents = "hello world";
    FileUtils.atomicallyReplace(filePath, contents);

    Assert.assertEquals(contents, Files.readString(filePath));
    assertOnlyOneFile(root, fileName);
  }

  @Test
  public void testAtomicallyReplaceWithByteContents() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();

    String fileName = "file.txt";
    Path filePath = getChildFilePath(root.toPath(), fileName);
    byte[] contents = "hello world".getBytes();
    FileUtils.atomicallyReplace(filePath, contents);

    Assert.assertArrayEquals(contents, Files.readAllBytes(filePath));
    assertOnlyOneFile(root, fileName);
  }

  @Test
  public void testAtomicallyReplaceOverwriteExistingFile() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    File root = tempDir.getRoot();

    String fileName = "file.txt";
    Path filePath = getChildFilePath(root.toPath(), fileName);
    String originalContents = "hello world";
    Files.writeString(filePath, originalContents);

    String overwrittenContents = "foobar";
    FileUtils.atomicallyReplace(filePath, overwrittenContents);

    Assert.assertEquals(overwrittenContents, Files.readString(filePath));
    assertOnlyOneFile(root, fileName);
  }

  private static Path getChildFilePath(Path dir, String fileName) {
    return dir.resolve(fileName);
  }

  private static void assertOnlyOneFile(File dir, String fileName) {
    File[] files = dir.listFiles();
    Assert.assertEquals(1, files.length);
    Assert.assertEquals(fileName, files[0].toPath().getFileName().toString());
  }
}
