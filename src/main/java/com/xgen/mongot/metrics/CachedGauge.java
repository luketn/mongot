package com.xgen.mongot.metrics;

import com.google.common.base.Ticker;
import com.google.common.collect.Comparators;
import com.google.errorprone.annotations.Var;
import java.time.Duration;
import java.util.function.ToDoubleFunction;

/**
 * This class is the ToDoubleFunction analog of Suppliers.memoizeWithExpiration intended to be used
 * with IO or CPU intensive micrometer metrics. On the first call, `applyAsDouble` computes the
 * delegate double function as normal and caches the result. Any subsequent calls to `applyAsDouble`
 * within the configured `expirationTime` will return this cached value.
 */
public class CachedGauge<T> implements ToDoubleFunction<T> {

  private static final Duration MIN_EXPIRATION_TIME = Duration.ofNanos(1);
  private static final Duration MAX_EXPIRATION_TIME = Duration.ofDays(1);

  private final Object lock = new Object();

  private final ToDoubleFunction<T> delegate;
  private final Ticker ticker;
  private final long durationNanos;
  private volatile double value = Double.NaN;
  // The special value 0 means "not yet initialized".
  private volatile long expirationNanos = 0;

  private CachedGauge(ToDoubleFunction<T> delegate, Duration expirationTime, Ticker ticker) {
    this.delegate = delegate;
    this.ticker = ticker;
    // Clamp to acceptable range rather than throw RuntimeExceptions
    this.durationNanos = clamp(expirationTime, MIN_EXPIRATION_TIME, MAX_EXPIRATION_TIME).toNanos();
  }

  public static <T> CachedGauge<T> of(ToDoubleFunction<T> delegate, Duration expirationTime) {
    return new CachedGauge<>(delegate, expirationTime, Ticker.systemTicker());
  }

  /**
   * Returns a ToDoubleFunction that caches the instance supplied by the delegate and removes the
   * cached value after the specified time has passed. Subsequent calls to {@code getAsDouble()}
   * return the cached value if the expiration time has not passed. After the expiration time, a new
   * value is retrieved, cached, and returned.
   *
   * <p>The returned supplier is thread-safe. When the underlying delegate throws an exception then
   * this memoizing supplier will keep delegating calls until it returns valid data.
   *
   * @param expirationTime The length of time after a value is created that it should stop being
   *     returned by subsequent {@code getAsDouble()} calls. Duration must be strictly positive and
   *     less than day.
   * @param ticker The time source to use to decide whether expirationTime has passed. In
   *     production, this would normally be Ticker.system(), but a FakeTicker can be supplied for
   *     unit tests.
   */
  public static <T> CachedGauge<T> of(
      ToDoubleFunction<T> delegate, Duration expirationTime, Ticker ticker) {
    return new CachedGauge<>(delegate, expirationTime, ticker);
  }

  private static <T extends Comparable<T>> T clamp(T value, T min, T max) {
    return Comparators.min(Comparators.max(value, min), max);
  }

  @Override
  public double applyAsDouble(T state) {
    // Note: This class is largely copied from Guava's Supplier#memoizeWithExpiration to use a
    // robust form of double-check locking.
    @Var long nanos = this.expirationNanos;
    long now = this.ticker.read();
    if (nanos == 0 || now - nanos >= 0) {
      synchronized (this.lock) {
        if (nanos == this.expirationNanos) { // recheck for lost race
          double result = this.delegate.applyAsDouble(state);
          this.value = result;
          nanos = now + this.durationNanos;
          // In the very unlikely event that nanos is 0, set it to 1;
          // no one will notice 1 ns of tardiness.
          this.expirationNanos = (nanos == 0) ? 1 : nanos;
          return result;
        }
      }
    }
    return this.value;
  }
}
