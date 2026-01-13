package com.xgen.mongot.util;

import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import java.time.Duration;

public class CumulativeDuration {

  private Duration total = Duration.ZERO;

  public Duration getTotal() {
    return this.total;
  }

  public <T, E extends Exception> T accumulate(CheckedSupplier<T, E> operation) throws E {
    var start = System.nanoTime();
    T result = operation.get();
    this.accumulate(Duration.ofNanos(System.nanoTime() - start));
    return result;
  }

  public <E extends Exception> void accumulate(CheckedRunnable<E> operation) throws E {
    var start = System.nanoTime();
    operation.run();
    this.accumulate(Duration.ofNanos(System.nanoTime() - start));
  }

  private void accumulate(Duration duration) {
    this.total = this.total.plus(duration);
  }
}
