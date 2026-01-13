package com.xgen.mongot.util.concurrent;

import com.xgen.testing.TestUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileLockGuardTest {

  // Execution flow (main thread and background thread run concurrently):
  // Main Thread                    Background Thread
  // ─────────────────              ───────────────────
  // 1. Create latches & lock
  // 2. Start background thread
  // 3. Verify lock is free          [Thread starts]
  //    (can acquire FileLock)
  // 4. lockLatch.countDown() ──────→ 5. Wait for lockLatch
  //                                  6. Acquire lock via FileLockGuard
  //                                  7. lockedLatch.countDown() ──→
  // 8. Wait for lockedLatch ─────────────────────────┘
  // 9. Verify lock is held
  //    (cannot acquire FileLock)     [Lock is held here]
  // 10. unlockLatch.countDown() ───→ 11. Wait for unlockLatch
  //                                  12. Exit try-with-resources
  //                                  13. Lock released automatically
  //                                  14. unlockedLatch.countDown() ─→
  // 15. Wait for unlockedLatch ───────────────────────┘
  // 16. Verify lock is free
  //     (can acquire FileLock)       [Lock is free here]
  // 17. future.get() ──────────────→ 18. [Thread completes]
  //                                    [If exception occurred, it would be thrown here]
  @Test
  public void with_acquiresAndReleasesLock() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    Path lockFile = tempDir.getRoot().toPath().resolve("test.lock");

    CountDownLatch lockLatch = new CountDownLatch(1);
    CountDownLatch lockedLatch = new CountDownLatch(1);
    CountDownLatch unlockLatch = new CountDownLatch(1);
    CountDownLatch unlockedLatch = new CountDownLatch(1);

    // In a separate thread, use the file lock guard when the latch counts down.
    CompletableFuture<Void> future =
        CompletableFuture.runAsync(
            () -> {
              try {
                Assert.assertTrue(lockLatch.await(5, TimeUnit.SECONDS));
                try (FileLockGuard ignored = FileLockGuard.with(lockFile)) {
                  lockedLatch.countDown();

                  Assert.assertTrue(unlockLatch.await(5, TimeUnit.SECONDS));
                }
                unlockedLatch.countDown();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            Executors.newSingleThreadExecutor());

    // Ensure that we can lock prior to the file lock guard.
    try (FileLockGuard ignored = FileLockGuard.with(lockFile)) {
      // Lock acquired successfully.
    }

    // Signal the thread to create the file lock guard and wait to be told it did.
    lockLatch.countDown();
    Assert.assertTrue(lockedLatch.await(5, TimeUnit.SECONDS));

    // Ensure we cannot lock while the FileLockGuard exists.
    Assert.assertThrows(IOException.class, () -> FileLockGuard.with(lockFile));

    // Signal the thread to drop the file lock guard and wait to be told it did.
    unlockLatch.countDown();
    Assert.assertTrue(unlockedLatch.await(5, TimeUnit.SECONDS));

    // Ensure we can lock again once the file lock guard is destroyed.
    try (FileLockGuard ignored = FileLockGuard.with(lockFile)) {
      // Lock acquired successfully.
    }

    // Ensure nothing went wrong in the future thread.
    future.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void with_whenParentDirectoryMissing_throwsIoException() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    Path nonExistentDir = tempDir.getRoot().toPath().resolve("nonexistent");
    Path lockFile = nonExistentDir.resolve("test.lock");

    // Acquiring the lock with FileLockGuard throws because parent directory doesn't exist.
    Assert.assertThrows(IOException.class, () -> FileLockGuard.with(lockFile));
  }

  @Test
  public void with_multipleLocks_mutuallyIndependent() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    Path lockFile1 = tempDir.getRoot().toPath().resolve("test1.lock");
    Path lockFile2 = tempDir.getRoot().toPath().resolve("test2.lock");

    try (FileLockGuard ignored1 = FileLockGuard.with(lockFile1)) {
      // Lock 1 acquired successfully.
      try (FileLockGuard ignored2 = FileLockGuard.with(lockFile2)) {
        // Lock 2 acquired successfully.
        Assert.assertThrows(IOException.class, () -> FileLockGuard.with(lockFile1));
      }
    }

    // After closing, the lock should be available again
    try (FileLockGuard ignored = FileLockGuard.with(lockFile1)) {
      // Lock 1 acquired successfully.
    }
    try (FileLockGuard ignored = FileLockGuard.with(lockFile2)) {
      // Lock 2 acquired successfully.
    }
  }

  @Test
  public void withPersistent_acquiresLock() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    Path lockFile = tempDir.getRoot().toPath().resolve("test.lock");

    // Acquire lock using withPersistent (doesn't require try-with-resources)
    FileLockGuard guard = FileLockGuard.withPersistent(lockFile);

    // Verify that another process cannot acquire the lock while it's held
    Assert.assertThrows(IOException.class, () -> FileLockGuard.with(lockFile));

    // Explicitly close the lock (even though withPersistent doesn't require it)
    guard.close();

    // After closing, the lock should be available again
    try (FileLockGuard ignored = FileLockGuard.with(lockFile)) {
      // Lock acquired successfully.
    }
  }

  @Test
  public void withPersistent_whenParentDirectoryMissing_throwsIoException() throws Exception {
    TemporaryFolder tempDir = TestUtils.getTempFolder();
    Path nonExistentDir = tempDir.getRoot().toPath().resolve("nonexistent");
    Path lockFile = nonExistentDir.resolve("test.lock");

    // Acquiring the lock with withPersistent throws because parent directory doesn't exist.
    Assert.assertThrows(IOException.class, () -> FileLockGuard.withPersistent(lockFile));
  }
}
