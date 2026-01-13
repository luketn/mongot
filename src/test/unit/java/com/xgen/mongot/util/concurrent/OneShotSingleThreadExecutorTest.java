package com.xgen.mongot.util.concurrent;

import java.util.concurrent.CompletableFuture;
import org.junit.Assert;
import org.junit.Test;

public class OneShotSingleThreadExecutorTest {

  @Test
  public void testCanRunTaskOnDifferentThread() throws Exception {
    OneShotSingleThreadExecutor executor = new OneShotSingleThreadExecutor("test");

    // Any variables referenced in a lambda have to be final, so we resort to internally mutating
    // an instance of a class.
    class RanCanary {
      private volatile boolean ran = false;
    }

    RanCanary canary = new RanCanary();
    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> canary.ran = true, executor);

    future.get();
    Assert.assertTrue("task did not run", canary.ran);
  }

  @Test
  public void testOnlyOneTask() {
    OneShotSingleThreadExecutor executor = new OneShotSingleThreadExecutor("test");

    executor.execute(
        () -> {
          // do nothing
        });

    try {
      executor.execute(
          () -> {
            // do nothing
          });
      Assert.fail("OneShotSingleThreadExecutor allowed multiple tasks to be executed");
    } catch (IllegalStateException e) {
      // expected
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testMustExecuteBeforeAccessingThread() {
    OneShotSingleThreadExecutor executor = new OneShotSingleThreadExecutor("test");
    executor.getThread();
  }

  @Test
  public void testReturnsInterruptableThread() throws Exception {
    OneShotSingleThreadExecutor executor = new OneShotSingleThreadExecutor("test");

    // Any variables referenced in a lambda have to be final, so we resort to internally mutating
    // an instance of a class.
    class InterruptedCanary {
      private volatile boolean interrupted = false;
    }

    InterruptedCanary canary = new InterruptedCanary();
    executor.execute(
        () -> {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            canary.interrupted = true;
          }
        });

    Thread thread = executor.getThread();
    thread.interrupt();
    thread.join();
    Assert.assertFalse("executor thread did not terminate", thread.isAlive());
    Assert.assertTrue("executor thread was not interrupted", canary.interrupted);
  }
}
