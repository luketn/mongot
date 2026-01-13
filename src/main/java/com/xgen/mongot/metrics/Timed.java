package com.xgen.mongot.metrics;

import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier2;
import io.micrometer.core.instrument.Timer;

public class Timed {

  private Timed() {}

  /** Runs the given supplier, timing its execution with the given timer. */
  public static <T, E extends Exception> T supplier(Timer timer, CheckedSupplier<T, E> supplier)
      throws E {
    Timer.Sample sample = Timer.start();
    T val = supplier.get();
    sample.stop(timer);
    return val;
  }

  public static <T, E1 extends Exception, E2 extends Exception> T supplier2(
      Timer timer, CheckedSupplier2<T, E1, E2> supplier) throws E1, E2 {
    Timer.Sample sample = Timer.start();
    T val = supplier.get();
    sample.stop(timer);
    return val;
  }

  /** Runs the given runnable, timing its execution with the given timer. */
  public static <E extends Exception> void runnable(Timer timer, CheckedRunnable<E> runnable)
      throws E {
    Timer.Sample sample = Timer.start();
    runnable.run();
    sample.stop(timer);
  }
}
