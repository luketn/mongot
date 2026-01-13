package com.xgen.mongot.util;

import static com.google.common.truth.Truth.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DirectorySizeTest {
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final byte[] DATA = "x".repeat(10_000).getBytes(StandardCharsets.UTF_8);

  @Test
  public void directorySizeTest() throws Exception {
    try (var fos = new FileOutputStream(new File(this.tempFolder.getRoot(), "data"))) {
      fos.write(DATA);
    }
    try (var fos = new FileOutputStream(new File(this.tempFolder.newFolder("subdir"), "data"))) {
      fos.write(DATA);
    }

    assertThat(DirectorySize.of(this.tempFolder.getRoot())).isEqualTo(20_000);
  }
}
