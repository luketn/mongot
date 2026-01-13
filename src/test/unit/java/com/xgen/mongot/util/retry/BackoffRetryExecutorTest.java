package com.xgen.mongot.util.retry;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class BackoffRetryExecutorTest {

  private static final Executor EXECUTOR = Executors.newSingleThreadExecutor();

  private static final ExponentialBackoffPolicy EXPONENTIAL_BACKOFF_POLICY =
      ExponentialBackoffPolicy.builder()
          .initialDelay(Duration.ofMillis(1))
          .backoffFactor(2)
          .maxDelay(Duration.ofMillis(100))
          .maxRetries(5)
          .jitter(0.1)
          .build();

  private static final FixedBackoffPolicy FIXED_BACKOFF_POLICY =
      FixedBackoffPolicy.builder().delay(Duration.ofMillis(1)).maxRetries(5).jitter(0.1).build();

  @Parameterized.Parameters(name = "{0}")
  public static List<Object[]> params() {
    return Arrays.asList(
        new BackoffPolicy[] {EXPONENTIAL_BACKOFF_POLICY},
        new BackoffPolicy[] {FIXED_BACKOFF_POLICY});
  }

  private final BackoffPolicy backoffPolicy;

  public BackoffRetryExecutorTest(BackoffPolicy backoffPolicy) {
    this.backoffPolicy = backoffPolicy;
  }

  @Test
  public void testImmediatelySucceeds() {
    var onRetryCounter = new AtomicInteger();
    var executor =
        new BackoffRetryExecutor(
            this.backoffPolicy, Optional.of(e -> onRetryCounter.incrementAndGet()));
    var counter = new AtomicInteger();
    var result = executor.get(counter::incrementAndGet, RuntimeException.class);

    Assert.assertEquals(0, onRetryCounter.intValue());
    Assert.assertEquals(1, result.intValue());
    Assert.assertEquals(1, counter.intValue());
  }

  @Test
  public void testSucceedsAfterAttempts() throws Exception {
    var onRetryCounter = new AtomicInteger();
    var executor =
        new BackoffRetryExecutor(
            this.backoffPolicy, Optional.of(e -> onRetryCounter.incrementAndGet()));
    var counter = new AtomicInteger();
    var result =
        executor.get(
            () -> {
              var count = counter.incrementAndGet();
              if (count <= 3) {
                throw new CheckedException();
              }

              return count;
            },
            CheckedException.class);

    Assert.assertEquals(3, onRetryCounter.intValue());
    Assert.assertEquals(4, result.intValue());
    Assert.assertEquals(4, counter.intValue());
  }

  @Test
  public void testThrowsUnexpectedException() {
    var onRetryCounter = new AtomicInteger();
    var executor =
        new BackoffRetryExecutor(
            this.backoffPolicy, Optional.of(e -> onRetryCounter.incrementAndGet()));
    var counter = new AtomicInteger();
    Assert.assertThrows(
        UncheckedException.class,
        () ->
            executor.get(
                () -> {
                  counter.incrementAndGet();
                  throw new UncheckedException();
                },
                CheckedException.class));

    Assert.assertEquals(0, onRetryCounter.intValue());
    Assert.assertEquals(1, counter.intValue());
  }

  @Test
  public void testGetAsyncSucceeds() throws ExecutionException, InterruptedException {
    var onRetryCounter = new AtomicInteger();
    var executor =
        new BackoffRetryExecutor(
            this.backoffPolicy, Optional.of(e -> onRetryCounter.incrementAndGet()));
    var counter = new AtomicInteger();
    CompletableFuture<Integer> result =
        executor.getAsync(counter::incrementAndGet, RuntimeException.class, EXECUTOR);
    Assert.assertEquals(1, result.get().intValue());
    Assert.assertEquals(0, onRetryCounter.intValue());
    Assert.assertEquals(1, counter.intValue());
  }

  @Test
  public void testGetAsyncThrowsUnexpectedException() {
    var onRetryCounter = new AtomicInteger();
    var executor =
        new BackoffRetryExecutor(
            this.backoffPolicy, Optional.of(e -> onRetryCounter.incrementAndGet()));
    var counter = new AtomicInteger();
    Assert.assertThrows(
        CompletionException.class,
        () ->
            executor
                .getAsync(
                    () -> {
                      counter.incrementAndGet();
                      throw new UncheckedException();
                    },
                    CheckedException.class,
                    EXECUTOR)
                .join());
    Assert.assertEquals(0, onRetryCounter.intValue());
    Assert.assertEquals(1, counter.intValue());
  }

  @Test
  public void testGetAsyncSucceedsAfterAttempts() {
    var onRetryCounter = new AtomicInteger();
    var executor =
        new BackoffRetryExecutor(
            this.backoffPolicy, Optional.of(e -> onRetryCounter.incrementAndGet()));
    var counter = new AtomicInteger();
    var result =
        executor.getAsync(
            () -> {
              var count = counter.incrementAndGet();
              if (count <= 3) {
                throw new CheckedException();
              }

              return count;
            },
            CheckedException.class,
            EXECUTOR);
    Assert.assertEquals(4, result.join().intValue());
    Assert.assertEquals(3, onRetryCounter.intValue());
    Assert.assertEquals(4, counter.intValue());
  }

  private static class CheckedException extends Exception {}

  private static class UncheckedException extends RuntimeException {}
}
