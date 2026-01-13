package com.xgen.mongot.util.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class ExecutorsTest {

  @Test
  public void testMetricsRemoved() {
    var meterRegistry = new SimpleMeterRegistry();
    Map<String, Meter> firstMeters = new HashMap<>();
    Map<String, Meter> secondMeters = new HashMap<>();
    try (var executor = Executors.fixedSizeThreadScheduledExecutor("test", 1, meterRegistry)) {
      executor.schedule(() -> {}, 10, TimeUnit.MINUTES);
      for (var entry : NamedExecutorService.METRICS.entrySet()) {
        var name = entry.getKey();
        var type = entry.getValue();
        Meter meter =
            switch (type) {
              case GAUGE -> meterRegistry.find("test." + name).gauge();
              case COUNTER -> meterRegistry.find("test." + name).functionCounter();
              case TIMER -> meterRegistry.find("test." + name).timer();
              default -> throw new IllegalStateException("Test does not support this metric type");
            };
        // Some metrics might not be present for all executor types
        if (meter != null) {
          firstMeters.put(name, meter);
        }
      }

      // Sanity check on one metric
      Meter poolSizeGauge = firstMeters.get("executor.pool.size");
      assertNotNull(poolSizeGauge);
      assertTrue(poolSizeGauge instanceof Gauge);
      assertEquals(1, ((Gauge) poolSizeGauge).value(), 0);
    }

    try (var executor = Executors.fixedSizeThreadScheduledExecutor("test", 2, meterRegistry)) {
      executor.schedule(() -> {}, 10, TimeUnit.MINUTES);
      executor.schedule(() -> {}, 10, TimeUnit.MINUTES);
      for (var entry : NamedExecutorService.METRICS.entrySet()) {
        var name = entry.getKey();
        var type = entry.getValue();
        Meter meter =
            switch (type) {
              case GAUGE -> meterRegistry.find("test." + name).gauge();
              case COUNTER -> meterRegistry.find("test." + name).functionCounter();
              case TIMER -> meterRegistry.find("test." + name).timer();
              default -> throw new IllegalStateException("Test does not support this metric type");
            };
        // Some metrics might not be present for all executor types
        if (meter != null) {
          secondMeters.put(name, meter);
        }
      }

      Meter poolSizeGauge = secondMeters.get("executor.pool.size");
      assertNotNull(poolSizeGauge);
      assertTrue(poolSizeGauge instanceof Gauge);
      // If metrics are not removed, the pool size will likely be stuck at 1 or 0
      assertEquals(2, ((Gauge) poolSizeGauge).value(), 0);
    }

    // Make sure that metrics were re-created
    for (var meterName : firstMeters.keySet()) {
      assertNotSame(firstMeters.get(meterName), secondMeters.get(meterName));
    }
  }
}
