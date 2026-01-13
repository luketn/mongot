package com.xgen.testing;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.functionalinterfaces.CheckedFunction;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;

public class ConcurrencyTestUtils {

  /**
   * Tests that all of the supplied invocations can be concurrently run.
   *
   * <p>The supplied invocations must have the property that they block on the supplied barrier
   * within whatever critical sections are being tested.
   *
   * <p>Additionally, the supplied barrier should be initialized with one more party than the number
   * of invocations so that assertCanBeInvokedConcurrently can block on it as well.
   */
  @SafeVarargs
  public static void assertCanBeInvokedConcurrently(
      CyclicBarrier barrier, CheckedRunnable<Exception>... invocations) throws Exception {
    Check.checkState(
        barrier.getParties() == invocations.length + 1,
        "barrier must wait on all invocations plus an additional party");

    Executor executor = Executors.newCachedThreadPool();
    List<CompletableFuture<?>> invocationFutures =
        Arrays.stream(invocations)
            .map(invocation -> FutureUtils.checkedRunAsync(invocation, executor, Exception.class))
            .collect(Collectors.toList());

    // Wait until all of the invocations have reached the barrier.
    barrier.await();

    // Wait for all of the invocations to complete and ensure no error occurred.
    FutureUtils.allOf(invocationFutures).get();
  }

  /**
   * Tests that calling invocation multiple times can happen concurrently.
   *
   * <p>inCriticalSectionCall should be a Mockito.when() invocation on a method call of a dependent
   * object that gets invoked within the critical section we're testing.
   *
   * <p>For example, if you want to test that some method foo() can be called concurrently, and
   * foo() invokes some dependency object of type Bar's method qux(), you may spy() the Bar method
   * passed in's qux method to hang on the barrier: <code>
   *   final Bar myBar = Mockito.spy(new Bar());
   *   final Foo myFoo = new Foo(myBar);
   *
   *   assertCanBeInvokedConcurrently(when(myBar.qux(), myFoo::foo);
   * </code>
   */
  public static <T> void assertCanBeInvokedConcurrently(
      OngoingStubbing<T> inCriticalSectionCall, CheckedRunnable<Exception> invocation)
      throws Exception {
    CyclicBarrier barrier = new CyclicBarrier(4);
    inCriticalSectionCall.then(awaitBarrier(barrier));

    assertCanBeInvokedConcurrently(barrier, invocation, invocation, invocation);
  }

  /** Tests that calling a mocked function can happen concurrently, similar to above. */
  public static <T, R> void assertCanBeInvokedConcurrently(
      T mock,
      CheckedFunction<T, R, Exception> inCriticalSectionCall,
      CheckedRunnable<Exception> invocation)
      throws Exception {
    CyclicBarrier barrier = new CyclicBarrier(4);
    inCriticalSectionCall.apply(Mockito.doAnswer(awaitBarrier(barrier)).when(mock));
    assertCanBeInvokedConcurrently(barrier, invocation, invocation, invocation);
  }

  @SuppressWarnings("unchecked")
  private static <T> Answer<T> awaitBarrier(CyclicBarrier barrier) {
    return invocation -> {
      barrier.await();
      return (T) invocation.callRealMethod();
    };
  }

  /**
   * Tests that the second runnable will block and not return until the first runnable has
   * completed.
   *
   * <p>The first runnable must have the property that within the critical section it is testing it
   * signals that it is there by counting down firstReadyToFinish, then blocks within the critical
   * section awaiting on firstShouldFinish.
   */
  public static void assertCannotBeInvokedConcurrently(
      CheckedRunnable<Exception> first,
      CountDownLatch firstReadyToFinish,
      CountDownLatch firstShouldFinish,
      CheckedRunnable<Exception> second)
      throws Exception {
    CountDownLatch finishedFirst = new CountDownLatch(1);
    Executor executor = Executors.newCachedThreadPool();

    // Run the first runnable.
    CompletableFuture<Void> firstFuture =
        FutureUtils.checkedRunAsync(
            () -> {
              first.run();
              finishedFirst.countDown();
            },
            executor,
            Exception.class);

    // Wait until the first runnable is blocked within the critical section.
    firstReadyToFinish.await();

    // Start running the second runnable.
    CountDownLatch finishedSecond = new CountDownLatch(1);
    CompletableFuture<Void> secondFuture =
        FutureUtils.checkedRunAsync(
            () -> {
              second.run();
              finishedSecond.countDown();
            },
            executor,
            Exception.class);

    // Ensure that the second runnable does not return.
    Assert.assertFalse(finishedSecond.await(1, TimeUnit.SECONDS));

    // Signal to finish the first runnable, then ensure that the second runnable completes.
    firstShouldFinish.countDown();
    finishedFirst.await();
    Assert.assertTrue(finishedSecond.await(1, TimeUnit.SECONDS));

    // Ensure there were no problems in the futures.
    CompletableFuture.allOf(firstFuture, secondFuture).get();
  }

  /**
   * Tests that the second runnable will block and not return until the first runnable has
   * completed.
   *
   * <p>inCriticalSectionCall should be a Mockito.when() invocation on a method call of a dependent
   * object that gets invoked within the critical section we're testing.
   *
   * <p>For example, if you want to test that some method first() can be called concurrently with
   * some method second(), and first() invokes some dependency object of type Bar's method qux(),
   * you may spy() the Bar method passed in's qux method to hang on the barrier: <code>
   *   final Bar myBar = Mockito.spy(new Bar());
   *   final Foo myFoo = new Foo(myBar);
   *
   *   assertCannotBeInvokedConcurrently(when(myBar.qux()), myFoo::first, myFoo::second);
   * </code>
   */
  public static <T> void assertCannotBeInvokedConcurrently(
      OngoingStubbing<T> inCriticalSectionCall,
      CheckedRunnable<Exception> first,
      CheckedRunnable<Exception> second)
      throws Exception {
    CountDownLatch firstReadyToStart = new CountDownLatch(1);
    CountDownLatch firstShouldStart = new CountDownLatch(1);
    inCriticalSectionCall.then(awaitInvocationSignal(firstReadyToStart, firstShouldStart));

    assertCannotBeInvokedConcurrently(first, firstReadyToStart, firstShouldStart, second);
  }

  /**
   * Tests that the second runnable will block and not return until the first runnable has
   * completed, similar to above.
   */
  public static <T, R> void assertCannotBeInvokedConcurrently(
      T mock,
      CheckedFunction<T, R, Exception> inCriticalSectionCall,
      CheckedRunnable<Exception> first,
      CheckedRunnable<Exception> second)
      throws Exception {
    CountDownLatch firstReadyToStart = new CountDownLatch(1);
    CountDownLatch firstShouldStart = new CountDownLatch(1);
    inCriticalSectionCall.apply(
        Mockito.doAnswer(awaitInvocationSignal(firstReadyToStart, firstShouldStart)).when(mock));

    assertCannotBeInvokedConcurrently(first, firstReadyToStart, firstShouldStart, second);
  }

  @SuppressWarnings("unchecked")
  private static <T> Answer<T> awaitInvocationSignal(
      CountDownLatch readyToStart, CountDownLatch shouldStart) {
    return invocation -> {
      readyToStart.countDown();
      shouldStart.await();
      return (T) invocation.callRealMethod();
    };
  }
}
