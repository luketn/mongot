package com.xgen.mongot.metrics;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.google.common.testing.FakeTicker;
import com.xgen.testing.TestUtils;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToDoubleFunction;
import org.junit.Test;

public class CachedGaugeTest {

  @Test
  public void create_eternalDuration_doesNotThrow() {
    CachedGauge.of(x -> 5, ChronoUnit.FOREVER.getDuration(), FakeTicker.systemTicker());
  }

  @Test
  public void apply_zeroDuration_invalidatesAfter1ns() {
    FakeTicker ticker = new FakeTicker().advance(ThreadLocalRandom.current().nextInt());
    var counter = new AtomicInteger();
    ToDoubleFunction<Object> countingSupplier = x -> counter.incrementAndGet();
    CachedGauge<Object> gauge = CachedGauge.of(countingSupplier, Duration.ofNanos(0), ticker);

    // Gauge is always evaluated on first call
    assertEquals(1, gauge.applyAsDouble(this), TestUtils.EPSILON);
    assertEquals(1, gauge.applyAsDouble(this), TestUtils.EPSILON);
    // Advance time, but not enough to invalidate cache
    ticker.advance(1);
    assertEquals(2, gauge.applyAsDouble(this), TestUtils.EPSILON);
  }

  @Test
  public void applyAsDouble_successCall_cachesResultUntilDurationExpires() {
    FakeTicker ticker = new FakeTicker().advance(ThreadLocalRandom.current().nextInt());
    var counter = new AtomicInteger();
    ToDoubleFunction<Object> countingSupplier = x -> counter.incrementAndGet();
    CachedGauge<Object> gauge = CachedGauge.of(countingSupplier, Duration.ofNanos(10), ticker);

    // Gauge is always evaluated on first call
    assertEquals(1, gauge.applyAsDouble(this), TestUtils.EPSILON);
    assertEquals(1, gauge.applyAsDouble(this), TestUtils.EPSILON);
    // Advance time, but not enough to invalidate cache
    ticker.advance(9);
    assertEquals(1, gauge.applyAsDouble(this), TestUtils.EPSILON);
    // Advance time exactly enough to invalidate cache
    ticker.advance(1);
    assertEquals(2, gauge.applyAsDouble(this), TestUtils.EPSILON);
    // Advance time to invalidate cache
    ticker.advance(10);
    assertEquals(3, gauge.applyAsDouble(this), TestUtils.EPSILON);
    assertEquals(3, counter.get());
  }

  @Test
  public void applyAsDouble_thrownException_doesNotCacheResult() {
    FakeTicker ticker = new FakeTicker().advance(ThreadLocalRandom.current().nextInt());
    ToDoubleFunction<Object> countingSupplier =
        x -> {
          throw new IllegalStateException("Test Exception");
        };
    CachedGauge<Object> gauge = CachedGauge.of(countingSupplier, Duration.ofMillis(10), ticker);

    Exception e1 = assertThrows(IllegalStateException.class, () -> gauge.applyAsDouble(this));
    Exception e2 = assertThrows(IllegalStateException.class, () -> gauge.applyAsDouble(this));

    assertThat(e1).hasMessageThat().isEqualTo("Test Exception");
    assertThat(e2).hasMessageThat().isEqualTo("Test Exception");
  }
}
