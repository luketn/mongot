package com.xgen.mongot.util.retry;

import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.util.Optional;
import java.util.function.Consumer;

public class Retry {

  public static BackoffRetryExecutorBuilder withBackoff(BackoffPolicy policy) {
    return new BackoffRetryExecutorBuilder(policy);
  }

  public static class BackoffRetryExecutorBuilder {

    private final BackoffPolicy policy;
    private Optional<Consumer<Throwable>> onRetry;

    public BackoffRetryExecutorBuilder(BackoffPolicy policy) {
      this.policy = policy;
      this.onRetry = Optional.empty();
    }

    public BackoffRetryExecutorBuilder onRetry(Consumer<Throwable> onRetry) {
      this.onRetry = Optional.of(onRetry);
      return this;
    }

    public BackoffRetryExecutor build() {
      return new BackoffRetryExecutor(this.policy, this.onRetry);
    }

    public <T, E extends Exception> T get(CheckedSupplier<T, E> supplier, Class<E> exceptionClass)
        throws E {
      return build().get(supplier, exceptionClass);
    }
  }
}
