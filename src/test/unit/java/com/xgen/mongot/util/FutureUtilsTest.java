package com.xgen.mongot.util;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.concurrent.OneShotSingleThreadExecutor;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.junit.Assert;
import org.junit.Test;

public class FutureUtilsTest {

  @Test
  public void testSwallowedFutureNoCallbackSuccessfulSource() {
    CompletableFuture<Void> future =
        FutureUtils.swallowedFuture(CompletableFuture.completedFuture(null));

    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
  }

  @Test
  public void testSwallowedFutureNoCallbackExceptionalSource() {
    RuntimeException exception = new RuntimeException();
    CompletableFuture<Void> future =
        FutureUtils.swallowedFuture(CompletableFuture.failedFuture(exception));

    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
  }

  @Test
  public void testSwallowedFutureWithCallbackSuccessfulSource() {
    AtomicInteger counter = new AtomicInteger();
    Consumer<Throwable> callback = t -> counter.incrementAndGet();
    CompletableFuture<Void> future =
        FutureUtils.swallowedFuture(CompletableFuture.completedFuture(null), callback);

    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
    assertEquals(0, counter.get());
  }

  @Test
  public void testSwallowedFutureWithCallbackExceptionalSource() {
    AtomicInteger counter = new AtomicInteger();
    Consumer<Throwable> callback = t -> counter.incrementAndGet();
    RuntimeException exception = new RuntimeException();
    CompletableFuture<Void> future =
        FutureUtils.swallowedFuture(CompletableFuture.failedFuture(exception), callback);

    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
    assertEquals(1, counter.get());
  }

  @Test
  public void testGetAndSwallowWhenExceptionOccurs() {
    var reference = new AtomicReference<Throwable>();
    var exception = new IllegalStateException("test");
    var future =
        CompletableFuture.<Void>supplyAsync(
            () -> {
              throw exception;
            });

    FutureUtils.getAndSwallow(future, reference::set);
    assertEquals(exception, reference.get().getCause());
  }

  @Test
  public void testCheckedRunAsyncRuns() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckedRunAsyncRuns");

    CountDownLatch latch = new CountDownLatch(1);
    CheckedRunnable<IOException> runnable = latch::countDown;
    CompletableFuture<Void> future =
        FutureUtils.checkedRunAsync(runnable, executor, IOException.class);

    latch.await(1, TimeUnit.SECONDS);
    future.get(1, TimeUnit.SECONDS);
  }

  @Test
  public void testCheckedRunAsyncThrowsChecked() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckedRunAsyncThrowsChecked");

    CountDownLatch latch = new CountDownLatch(1);
    IOException exception = new IOException("testCheckedRunAsyncThrowsChecked");
    CheckedRunnable<IOException> runnable =
        () -> {
          latch.countDown();
          throw exception;
        };

    assertCheckedFutureThrows(
        latch, exception, () -> FutureUtils.checkedRunAsync(runnable, executor, IOException.class));
  }

  @Test
  public void testCheckedRunAsyncThrowsUnchecked() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckRunAsyncThrowsChecked");

    CountDownLatch latch = new CountDownLatch(1);
    RuntimeException exception = new RuntimeException("testCheckRunAsyncThrowsUnchecked");
    CheckedRunnable<IOException> runnable =
        () -> {
          latch.countDown();
          throw exception;
        };

    assertCheckedFutureThrows(
        latch, exception, () -> FutureUtils.checkedRunAsync(runnable, executor, IOException.class));
  }

  @Test
  public void testCheckedRunAsyncThrowsError() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckedRunAsyncThrowsError");

    CountDownLatch latch = new CountDownLatch(1);
    AssertionError error = new AssertionError("testCheckedRunAsyncThrowsError");
    CheckedRunnable<IOException> runnable =
        () -> {
          latch.countDown();
          throw error;
        };

    assertCheckedFutureThrows(
        latch, error, () -> FutureUtils.checkedRunAsync(runnable, executor, IOException.class));
  }

  @Test
  public void testCheckedSupplyAsyncRuns() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckedSupplyAsyncRuns");

    Boolean expected = Boolean.TRUE;
    CheckedSupplier<Boolean, IOException> supplier = () -> expected;
    CompletableFuture<Boolean> future =
        FutureUtils.checkedSupplyAsync(supplier, executor, IOException.class);

    Assert.assertSame(expected, future.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void testCheckedSupplyAsyncThrowsChecked() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckedSupplyAsyncThrowsChecked");

    CountDownLatch latch = new CountDownLatch(1);
    IOException exception = new IOException("testCheckedSupplyAsyncThrowsChecked");
    CheckedSupplier<Boolean, IOException> supplier =
        () -> {
          latch.countDown();
          throw exception;
        };

    assertCheckedFutureThrows(
        latch,
        exception,
        () -> FutureUtils.checkedSupplyAsync(supplier, executor, IOException.class));
  }

  @Test
  public void testCheckedSupplyAsyncThrowsUnchecked() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckedSupplyAsyncThrowsUnchecked");

    CountDownLatch latch = new CountDownLatch(1);
    RuntimeException exception = new RuntimeException("testCheckedSupplyAsyncThrowsUnchecked");
    CheckedSupplier<Boolean, IOException> supplier =
        () -> {
          latch.countDown();
          throw exception;
        };

    assertCheckedFutureThrows(
        latch,
        exception,
        () -> FutureUtils.checkedSupplyAsync(supplier, executor, IOException.class));
  }

  @Test
  public void testCheckedSupplyAsyncThrowsError() throws Exception {
    OneShotSingleThreadExecutor executor =
        new OneShotSingleThreadExecutor("testCheckedSupplyAsyncThrowsError");

    CountDownLatch latch = new CountDownLatch(1);
    AssertionError error = new AssertionError("testCheckedSupplyAsyncThrowsError");
    CheckedSupplier<Boolean, IOException> supplier =
        () -> {
          latch.countDown();
          throw error;
        };

    assertCheckedFutureThrows(
        latch, error, () -> FutureUtils.checkedSupplyAsync(supplier, executor, IOException.class));
  }

  private void assertCheckedFutureThrows(
      CountDownLatch latch, Throwable expected, Supplier<CompletableFuture<?>> futureSupplier)
      throws Exception {
    CompletableFuture<?> future = futureSupplier.get();
    latch.await(1, TimeUnit.SECONDS);

    try {
      future.get(1, TimeUnit.SECONDS);
      Assert.fail("future didn't throw exception");
    } catch (ExecutionException e) {
      Assert.assertSame(expected, e.getCause());
    }
  }

  @Test
  public void testAwaitAllCompleteCompletedFuture() throws Exception {
    FutureUtils.awaitAllComplete(Duration.ofSeconds(1), CompletableFuture.completedFuture(null));
  }

  @Test
  public void testAwaitAllCompleteFailedFuture() throws Exception {
    FutureUtils.awaitAllComplete(
        Duration.ofSeconds(1), CompletableFuture.failedFuture(new RuntimeException()));
  }

  @Test
  public void testAwaitAllCompleteDelayedFuture() throws Exception {
    CompletableFuture<Void> future =
        CompletableFuture.supplyAsync(
            () -> null, CompletableFuture.delayedExecutor(10, TimeUnit.MILLISECONDS));
    FutureUtils.awaitAllComplete(Duration.ofSeconds(1), future);
  }

  @Test(expected = TimeoutException.class)
  public void testAwaitAllCompleteTimesOut() throws Exception {
    CompletableFuture<Void> delayed =
        CompletableFuture.supplyAsync(
            () -> null, CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS));
    FutureUtils.awaitAllComplete(Duration.ofMillis(10), delayed);
  }

  @Test
  public void testAwaitAllCompleteMultipleCompletedFutures() throws Exception {
    FutureUtils.awaitAllComplete(
        Duration.ofSeconds(1),
        CompletableFuture.completedFuture(null),
        CompletableFuture.completedFuture(null));
  }

  @Test
  public void testAwaitAllCompleteMultipleFailedFutures() throws Exception {
    FutureUtils.awaitAllComplete(
        Duration.ofSeconds(1),
        CompletableFuture.failedFuture(new RuntimeException()),
        CompletableFuture.failedFuture(new RuntimeException()));
  }

  @Test
  public void testAwaitAllCompleteMultipleDelayedDelayed() throws Exception {
    CompletableFuture<Void> delayedSuccess =
        CompletableFuture.supplyAsync(
            () -> null, CompletableFuture.delayedExecutor(10, TimeUnit.MILLISECONDS));
    CompletableFuture<Void> delayedFailure =
        CompletableFuture.supplyAsync(
            () -> {
              throw new RuntimeException();
            },
            CompletableFuture.delayedExecutor(10, TimeUnit.MILLISECONDS));

    FutureUtils.awaitAllComplete(Duration.ofSeconds(1), delayedSuccess, delayedFailure);
  }

  @Test(expected = TimeoutException.class)
  public void testAwaitAllCompleteTimesOutMultipleFutures() throws Exception {
    CompletableFuture<Void> delayed =
        CompletableFuture.supplyAsync(
            () -> null, CompletableFuture.delayedExecutor(1, TimeUnit.SECONDS));
    FutureUtils.awaitAllComplete(
        Duration.ofMillis(10), CompletableFuture.completedFuture(null), delayed);
  }

  @Test
  public void testTransposeListCompletedFuture() throws Exception {
    CompletableFuture<List<Integer>> future =
        FutureUtils.transposeList(
            Arrays.asList(
                CompletableFuture.supplyAsync(() -> 1), CompletableFuture.supplyAsync(() -> 2)));
    List<Integer> actual = future.get();
    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
    assertEquals(Arrays.asList(1, 2), actual);
  }

  @Test
  public void testTransposeListCompleteFailedFuture() {
    CompletableFuture<List<Integer>> future =
        FutureUtils.transposeList(
            Arrays.asList(
                CompletableFuture.supplyAsync(() -> 1),
                CompletableFuture.failedFuture(new RuntimeException("Computation failed."))));
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("Computation failed.", exception.getCause().getMessage());
    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
  }

  @Test
  public void testTransposeMapCompletedFuture() throws Exception {
    CompletableFuture<Map<String, Integer>> future =
        FutureUtils.transposeMap(
            Arrays.asList(
                CompletableFuture.supplyAsync(() -> ImmutableMap.of("1", 1)),
                CompletableFuture.supplyAsync(() -> ImmutableMap.of("2", 2))));
    Map<String, Integer> actual = future.get();
    Assert.assertTrue(future.isDone());
    Assert.assertFalse(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
    assertEquals(ImmutableMap.of("1", 1, "2", 2), actual);
  }

  @Test
  public void testTransposeMapCompleteFailedFuture() {
    CompletableFuture<Map<String, String>> future =
        FutureUtils.transposeMap(
            Arrays.asList(
                CompletableFuture.supplyAsync(() -> ImmutableMap.of("k1", "v1")),
                CompletableFuture.failedFuture(new RuntimeException("Computation failed."))));
    var exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals("Computation failed.", exception.getCause().getMessage());
    Assert.assertTrue(future.isDone());
    Assert.assertTrue(future.isCompletedExceptionally());
    Assert.assertFalse(future.isCancelled());
  }
}
