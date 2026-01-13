package com.xgen.mongot.util.concurrent;

import io.micrometer.core.instrument.Counter;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;

/** {@link CallerRunsPolicy} which increments the counter each time the rejection is triggered. */
public class MeteredCallerRunsPolicy extends CallerRunsPolicy {

  private final Counter counter;

  public MeteredCallerRunsPolicy(Counter rejectionCounter) {
    this.counter = rejectionCounter;
  }

  @Override
  public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
    this.counter.increment();
    super.rejectedExecution(r, e);
  }
}
