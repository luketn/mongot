package com.xgen.mongot.util;

import io.micrometer.core.instrument.Counter;
import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for registering phantom cleanup actions for AutoCloseable resources.
 *
 * <p>This class provides a way to register cleanup actions that will be automatically executed when
 * an object becomes phantom reachable (eligible for garbage collection) but hasn't been explicitly
 * closed. This helps detect resource leaks and ensures cleanup even when resources are not properly
 * closed.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * public class MyResource implements AutoCloseable {
 *   private static class MyCleanAction implements CloseablePhantomCleaner.NoRefCloseable {
 *     private final SomeExternalResource resource;
 *
 *     public MyCleanAction(SomeExternalResource resource) {
 *       this.resource = resource;
 *     }
 *
 *     @Override
 *     public void close() throws IOException {
 *       resource.release();
 *     }
 *   }
 *
 *   private final Closeable cleanable;
 *
 *   public MyResource(SomeExternalResource resource) {
 *     this.cleanable = CloseablePhantomCleaner.register(
 *         this,
 *         new CloseablePhantomCleaner.CleanerThreadAction(counter, new MyCleanAction(resource)));
 *   }
 *
 *   @Override
 *   public void close() throws IOException {
 *     this.cleanable.close();
 *   }
 * }
 * }</pre>
 *
 * <p><strong>Important:</strong> NoRefCloseable implementations must NOT hold strong references to
 * the object being cleaned, as this would prevent garbage collection. Always use static nested
 * classes for NoRefCloseable implementations. Do NOT use lambda expressions or anonymous classes as
 * they can capture implicit references to the enclosing instance.
 *
 * @see java.lang.ref.Cleaner
 */
public class CloseablePhantomCleaner {

  private static final Logger LOG = LoggerFactory.getLogger(CloseablePhantomCleaner.class);

  private static final Cleaner cleaner = Cleaner.create();

  // Implementation of this Closeable  must NOT hold strong references to the object being cleaned,
  // as this would prevent garbage collection
  public interface NoRefCloseable extends Closeable {}

  public static class CleanerThreadAction implements Runnable {
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Counter phantomCleanerCounter;
    private final NoRefCloseable cleaning;

    // accepts a counter to increase if and only if cleaning was done by the Cleaner thread + logic
    // to do actual cleaning itself
    public CleanerThreadAction(Counter phantomCleanerCounter, NoRefCloseable cleaning) {
      this.phantomCleanerCounter = phantomCleanerCounter;
      this.cleaning = cleaning;
    }

    private void cleanup(boolean fromCleaner) throws IOException {
      // Run only once
      if (!this.closed.compareAndSet(false, true)) {
        return;
      }

      if (fromCleaner) {
        this.phantomCleanerCounter.increment();
      }

      this.cleaning.close();
    }

    @Override
    public void run() {
      try {
        // Called by the Cleaner thread.
        cleanup(true);
      } catch (Exception e) {
        LOG.error("Failed to clean up", e);
      }
    }
  }

  // --- internal holder that pairs Action + Cleanable ---
  private record CleaningRegistration(CleanerThreadAction action, Cleaner.Cleanable cleanable)
      implements Closeable {

    @Override
    public void close() throws IOException {
      try {
        // not from cleaner thead
        this.action.cleanup(false);
      } finally {
        //  de-register itself
        this.cleanable.clean();
      }
    }
  }

  public static Closeable register(Object referent, CleanerThreadAction action) {
    Cleaner.Cleanable cleanable = cleaner.register(referent, action);
    return new CleaningRegistration(action, cleanable);
  }
}
