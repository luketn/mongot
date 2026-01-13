package com.xgen.mongot.util.concurrent;

import com.google.errorprone.annotations.MustBeClosed;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

/**
 * FileLockGuard enables FileLocks to be automatically released when exiting a try-with-resources
 * block.
 *
 * <p>Without a FileLockGuard, you must ensure that you always open the channel, acquire the lock,
 * and release both in a finally block like the following: <code>
 *   FileChannel channel =
 *      FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
 *   FileLock lock = channel.tryLock().orElseThrow();
 *   try {
 *     // critical section
 *   } finally {
 *     if (lock.isValid()) {
 *       lock.release();
 *     }
 *     channel.close();
 *   }
 * </code>
 *
 * <p>It can be easy to forget to release the lock, or in some cases where there may be multiple
 * locks being used, its possible to release the wrong lock on accident.
 *
 * <p>With a FileLockGuard, the proper lock is automatically released for you: <code>
 *   try (final FileLockGuard ignored = FileLockGuard.with(lockFile)) {
 *     // critical section
 *   } // FileLockGuard.close() called automatically.
 * </code>
 *
 * <p>Additionally, since FileLockGuard.with() is annotated with @MustBeClosed, you cannot misuse a
 * lock guard by not creating it in a try-with-resources block: <code>
 * error: [MustBeClosedChecker] The result of this method must be closed.
 *     final FileLockGuard ignored = FileLockGuard.with(lockFile);
 * </code>
 *
 * <p>This is similar to RAII lock guards in other languages (see
 * https://en.cppreference.com/w/cpp/thread/lock_guard).
 */
public final class FileLockGuard implements AutoCloseable {

  private final FileChannel channel;
  private final FileLock lock;

  private FileLockGuard(FileChannel channel, FileLock lock) {
    this.channel = channel;
    this.lock = lock;
  }

  /**
   * Acquires an exclusive file lock on the specified file and returns a FileLockGuard. The lock
   * will be automatically released when the guard is closed.
   *
   * <p>If the lock file does not exist, it will be created automatically by {@link
   * FileChannel#open}. However, the parent directory must already exist; if it doesn't, an {@link
   * IOException} will be thrown.
   *
   * @param lockFile the path to the lock file
   * @return a FileLockGuard that will release the lock when closed
   * @throws IOException if the lock cannot be acquired (e.g., another process holds the lock) or if
   *     the parent directory does not exist
   */
  @MustBeClosed
  public static FileLockGuard with(Path lockFile) throws IOException {
    return acquireLock(lockFile);
  }

  /**
   * Acquires an exclusive file lock on the specified file and returns a FileLockGuard. The lock
   * will be held for the lifetime of the process and automatically released by the OS when the
   * process exits.
   *
   * <p>This method is intended for use cases where the lock should be held for the entire lifetime
   * of the process (e.g., preventing multiple instances of a server from running). The lock will be
   * automatically released by the operating system when the process exits, so it should not be
   * explicitly closed.
   *
   * <p>If the lock file does not exist, it will be created automatically by {@link
   * FileChannel#open}. However, the parent directory must already exist; if it doesn't, an {@link
   * IOException} will be thrown.
   *
   * @param lockFile the path to the lock file
   * @return a FileLockGuard that will be released by the OS when the process exits
   * @throws IOException if the lock cannot be acquired (e.g., another process holds the lock) or if
   *     the parent directory does not exist
   */
  public static FileLockGuard withPersistent(Path lockFile) throws IOException {
    return acquireLock(lockFile);
  }

  /** Internal method to acquire the lock. */
  private static FileLockGuard acquireLock(Path lockFile) throws IOException {
    // FileChannel.open with CREATE will create the file if it doesn't exist.
    // If parent directory doesn't exist, FileChannel.open() will throw NoSuchFileException.
    FileChannel channel =
        FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    try {
      FileLock lock =
          Optional.ofNullable(channel.tryLock())
              .orElseThrow(
                  () ->
                      new IOException(
                          "Failed to acquire exclusive lock on "
                              + lockFile
                              + ". Another process may be running."));
      return new FileLockGuard(channel, lock);
    } catch (java.nio.channels.OverlappingFileLockException e) {
      // OverlappingFileLockException can be thrown when another thread/process in the same JVM
      // holds the lock. Wrap it in IOException for consistent error handling.
      channel.close();
      throw new IOException(
          "Failed to acquire exclusive lock on " + lockFile + ". Another process may be running.",
          e);
    } catch (IOException e) {
      // If we fail to acquire the lock, close the channel before throwing
      channel.close();
      throw e;
    }
  }

  @Override
  public void close() throws IOException {
    if (!this.lock.isValid()) {
      throw new IllegalStateException(
          "FileLockGuard lock is invalid. The lock may have been released or the channel closed "
              + "outside of FileLockGuard.");
    }
    this.lock.release();
    this.channel.close();
  }
}
