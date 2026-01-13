package com.xgen.mongot.util.timers;

import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

/**
 * Thread-safe implementation of an InvocationCountingTimer. Method calls to <code>
 * getInvocationCount()</code> <code>getElapsedNanos()</code> aren't guaranteed to be consistent
 * amongst themselves as you may observe an increase in one's return value compared to another.
 */
public class ThreadSafeInvocationCountingTimer implements InvocationCountingTimer {

  @FunctionalInterface
  public interface TimingDataCreator {
    TimingData createTimingData(long invocationCount, long elapsedNanos);
  }

  private final Supplier<Stopwatch> stopwatchSupplier;
  private final LongAdder elapsedNanos = new LongAdder();
  private final LongAdder invocationCount = new LongAdder();
  private final TimingDataCreator timingDataCreator;

  /** Initializes the CountingTimer with elapsedNanos and invocationCount. */
  public ThreadSafeInvocationCountingTimer(Ticker ticker, long elapsedNanos, long invocationCount) {
    this(ticker);
    this.elapsedNanos.add(elapsedNanos);
    this.invocationCount.add(invocationCount);
  }

  public ThreadSafeInvocationCountingTimer(Ticker ticker) {
    this(ticker, TimingData.InvocationCountTimingData::new);
  }

  public ThreadSafeInvocationCountingTimer(Ticker ticker, TimingDataCreator timingDataCreator) {
    this.stopwatchSupplier = () -> Stopwatch.createStarted(ticker);
    this.timingDataCreator = timingDataCreator;
  }

  public static ThreadSafeInvocationCountingTimer merge(
      Ticker ticker, InvocationCountingTimer first, InvocationCountingTimer second) {
    return new ThreadSafeInvocationCountingTimer(
        ticker,
        first.getElapsedNanos() + second.getElapsedNanos(),
        first.getInvocationCount() + second.getInvocationCount());
  }

  @Override
  public SafeClosable split() {
    var stopwatch = this.stopwatchSupplier.get();
    return () -> {
      this.elapsedNanos.add(stopwatch.stop().elapsed(TimeUnit.NANOSECONDS));
      this.invocationCount.increment();
    };
  }

  @Override
  public long getInvocationCount() {
    return this.invocationCount.longValue();
  }

  @Override
  public long getElapsedNanos() {
    return this.elapsedNanos.longValue();
  }

  @Override
  public TimingData getTimingData() {
    return this.timingDataCreator.createTimingData(getInvocationCount(), getElapsedNanos());
  }
}
