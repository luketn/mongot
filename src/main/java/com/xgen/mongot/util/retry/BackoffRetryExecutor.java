package com.xgen.mongot.util.retry;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;

public class BackoffRetryExecutor {

  private final BackoffPolicy policy;
  private final Optional<Consumer<Throwable>> onRetry;

  BackoffRetryExecutor(BackoffPolicy policy, Optional<Consumer<Throwable>> onRetry) {
    this.policy = policy;
    this.onRetry = onRetry;
  }

  /**
   * Executes the supplier with the backoff policy the BackoffRetryExecutor was initialized with.
   */
  public <T, E extends Exception> T get(CheckedSupplier<T, E> supplier, Class<E> exceptionClass)
      throws E {
    RetryPolicy<T> retryPolicy =
        new RetryPolicy<T>()
            .handle(exceptionClass)
            .onRetry(e -> this.onRetry.ifPresent(r -> r.accept(e.getLastFailure())));
    this.policy.applyParameters(retryPolicy);

    try {
      return Failsafe.with(retryPolicy).get(supplier::get);
    } catch (FailsafeException e) {
      Throwable cause = e.getCause();
      if (exceptionClass.isInstance(cause)) {
        throw exceptionClass.cast(cause);
      }

      Check.checkState(
          cause instanceof RuntimeException,
          "failsafe executor threw an unknown checked exception");
      throw (RuntimeException) cause;
    }
  }

  /**
   * Executes the supplier asynchronously with the backoff policy the BackoffRetryExecutor was
   * initialized with. Caller needs to handle exception and error in returned future.
   */
  public <T, E extends Exception> CompletableFuture<T> getAsync(
      CheckedSupplier<T, E> supplier, Class<E> exceptionClass, Executor executor) {
    RetryPolicy<T> retryPolicy =
        new RetryPolicy<T>()
            .handle(exceptionClass)
            .onRetry(e -> this.onRetry.ifPresent(r -> r.accept(e.getLastFailure())));
    this.policy.applyParameters(retryPolicy);
    return Failsafe.with(retryPolicy).with(executor).getAsync(supplier::get);
  }
}
