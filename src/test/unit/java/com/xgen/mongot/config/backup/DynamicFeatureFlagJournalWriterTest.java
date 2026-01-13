package com.xgen.mongot.config.backup;

import com.xgen.testing.TestUtils;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class DynamicFeatureFlagJournalWriterTest {
  @Test
  public void testPersistCreatesDirectoryIfDoesNotExist() throws Exception {
    Path nestedNonExistingPath =
        TestUtils.getTempFolder().getRoot().toPath().resolve(Paths.get("a", "b", "c", "file.txt"));
    var writer = new DynamicFeatureFlagJournalWriter(nestedNonExistingPath);
    writer.persist(new DynamicFeatureFlagJournal(List.of()));
    Assert.assertTrue(nestedNonExistingPath.toFile().exists());
  }
}
