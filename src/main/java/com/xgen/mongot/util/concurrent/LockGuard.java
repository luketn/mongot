package com.xgen.mongot.util.concurrent;

import com.google.errorprone.annotations.MustBeClosed;
import java.util.concurrent.locks.Lock;

/**
 * LockGuard enables explicit Locks to be automatically closed when exiting a try-with-resources
 * block.
 *
 * <p>Without a LockGuard, you must ensure that you always close a Lock in a finally block like the
 * following: <code>
 *   myLock.lock();
 *   try {
 *     // critical section
 *   } finally {
 *     myLock.unlock();
 *   }
 * </code>
 *
 * <p>It can be easy to forget to unlock the lock, or in some cases where there may be multiple
 * locks being used, its possible to unlock the wrong lock on accident.
 *
 * <p>With a LockGuard, the proper lock is automatically unlocked for you: <code>
 *   try (final LockGuard ignored = LockGuard.with(myLock)) {
 *     // critical section
 *   } // LockGuard.close() called automatically.
 * </code>
 *
 * <p>Additionally, since LockGuard.with() is annotated with @MustBeClosed, you cannot misuse a lock
 * guard by not creating it in a try-with-resources block: <code>
 * error: [MustBeClosedChecker] The result of this method must be closed.
 *     final LockGuard ignored = LockGuard.with(lock);
 * </code>
 *
 * <p>This is similar to RAII lock guards in other languages (see
 * https://en.cppreference.com/w/cpp/thread/lock_guard).
 */
public class LockGuard implements AutoCloseable {

  private final Lock lock;

  private LockGuard(Lock lock) {
    this.lock = lock;
  }

  /**
   * Constructs a new LockGuard from the supplied lock, locking the lock on behalf of the caller.
   */
  @MustBeClosed
  public static LockGuard with(Lock lock) {
    lock.lock();
    return new LockGuard(lock);
  }

  /**
   * Constructs a new LockGuard from the supplied lock, locking the lock on behalf of the caller.
   */
  @MustBeClosed
  public static LockGuard withInterruptibly(Lock lock) throws InterruptedException {
    lock.lockInterruptibly();
    return new LockGuard(lock);
  }

  /**
   * Constructs a new LockGuard from the supplied lock. Note that the caller must have acquired the
   * lock prior to calling withLocked().
   */
  @MustBeClosed
  public static LockGuard withLocked(Lock lock) {
    return new LockGuard(lock);
  }

  @Override
  public void close() {
    this.lock.unlock();
  }
}
