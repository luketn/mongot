package com.xgen.mongot.config.manager;

import com.xgen.mongot.config.backup.JournalWriter;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Assert;
import org.junit.Test;

public class JournalWriterTest {
  @Test
  public void testPersistCreatesDirectoryIfDoesNotExist() throws Exception {
    Path nestedNonExistingPath =
        TestUtils.getTempFolder().getRoot().toPath().resolve(Paths.get("a", "b", "c", "file.txt"));
    var writer = new JournalWriter(nestedNonExistingPath);
    writer.persist(ConfigJournalV1Builder.builder().build());
    Assert.assertTrue(nestedNonExistingPath.toFile().exists());
  }
}
