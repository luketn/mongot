package com.xgen.mongot.util.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.Assert;
import org.junit.Test;

public class LockGuardTest {

  // Execution flow (main thread and background thread run concurrently):
  // Main Thread                    Background Thread
  // ─────────────────              ───────────────────
  // 1. Create latches & lock
  // 2. Start background thread
  // 3. Verify lock is free          [Thread starts]
  //    (lock.tryLock() = true)
  // 4. lockLatch.countDown() ──────→ 5. Wait for lockLatch
  //                                  6. Acquire lock via LockGuard
  //                                  7. lockedLatch.countDown() ──→
  // 8. Wait for lockedLatch ─────────────────────────┘
  // 9. Verify lock is held
  //    (lock.tryLock() = false)     [Lock is held here]
  // 10. unlockLatch.countDown() ───→ 11. Wait for unlockLatch
  //                                  12. Exit try-with-resources
  //                                  13. Lock released automatically
  //                                  14. unlockedLatch.countDown() ─→
  // 15. Wait for unlockedLatch ───────────────────────┘
  // 16. Verify lock is free
  //     (lock.tryLock() = true)      [Lock is free here]
  // 17. future.get() ──────────────→ 18. [Thread completes]
  //                                    [If exception occurred, it would be thrown here]
  @Test
  public void with_acquiresAndReleasesLock() throws Exception {
    CountDownLatch lockLatch = new CountDownLatch(1);
    CountDownLatch lockedLatch = new CountDownLatch(1);
    CountDownLatch unlockLatch = new CountDownLatch(1);
    CountDownLatch unlockedLatch = new CountDownLatch(1);

    Lock lock = new ReentrantLock();

    // In a separate thread, use the file lock guard when the latch counts down.
    CompletableFuture<Void> future =
        CompletableFuture.runAsync(
            () -> {
              try {
                Assert.assertTrue(lockLatch.await(5, TimeUnit.SECONDS));
                try (LockGuard ignored = LockGuard.with(lock)) {
                  lockedLatch.countDown();

                  Assert.assertTrue(unlockLatch.await(5, TimeUnit.SECONDS));
                }
                unlockedLatch.countDown();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            },
            Executors.newSingleThreadExecutor());

    // Ensure that we can lock prior to the lock guard.
    Assert.assertTrue(lock.tryLock());
    lock.unlock();

    // Signal the thread to create the lock guard and wait to be told it did.
    lockLatch.countDown();
    Assert.assertTrue(lockedLatch.await(5, TimeUnit.SECONDS));

    // Ensure we cannot lock while the LockGuard exists.
    Assert.assertFalse(lock.tryLock());

    // Signal the thread to drop the lock guard and wait to be told it did.
    unlockLatch.countDown();
    Assert.assertTrue(unlockedLatch.await(5, TimeUnit.SECONDS));

    // Ensure we can lock again once the lock guard is destroyed.
    Assert.assertTrue(lock.tryLock());

    // Ensure nothing went wrong in the future thread.
    future.get(5, TimeUnit.SECONDS);
  }
}
