package com.xgen.mongot.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.CompileTimeConstant;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton global metric factory for metrics that should exist globally
 * across Mongot, e.g., unreachable code counters.
 *
 * <p>This version is self-contained and does not depend on MetricsFactory.
 */
public final class GlobalMetricFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(GlobalMetricFactory.class);
  private static final Duration[] DETAILED_TRACE_SPAN_BUCKETS = {
    micros(25),
    micros(50),
    micros(75),
    micros(100),
    micros(125),
    micros(150),
    micros(200),
    micros(250),
    micros(300),
    micros(400),
    micros(500),
    micros(750),
    millis(1),
    millis(2),
    millis(3),
    millis(4),
    millis(6),
    millis(8),
    millis(12),
    millis(16),
    millis(24),
    millis(32),
    millis(48),
    millis(64),
    millis(96),
    millis(128),
    millis(256),
    millis(512),
    Duration.ofSeconds(1)
  };

  private static volatile Optional<MeterRegistry> meterRegistry = Optional.empty();
  private static final ConcurrentMap<String, Timer> detailedTraceSpanTimers =
      new ConcurrentHashMap<>();

  private GlobalMetricFactory() {}

  /** Initialize the global metric factory. Should be called once at startup. */
  public static synchronized void initialize(MeterRegistry registry) {
    if (meterRegistry.isPresent()) {
      LOGGER.debug("GlobalMetricFactory already initialized.");
      return;
    }
    meterRegistry = Optional.of(registry);
    LOGGER.info("GlobalMetricFactory initialized successfully.");
  }

  /**
   * Returns an unreachable counter labeled by reason.
   *
   * @param reason the compile-time constant reason for the unreachable path
   * @param registry the initialized MeterRegistry
   * @return the counter corresponding to the given reason
   */
  private static Counter unreachableCounter(
      @CompileTimeConstant String reason, MeterRegistry registry) {
    return Counter.builder("mongot.unreachable_code_paths")
        .description("Counts unreachable code paths, labeled by reason")
        .tags(Tags.of("reason", reason))
        .register(registry);
  }

  /**
   * Increments the unreachable counter for a given reason.
   *
   * @param reason the compile-time constant reason for the unreachable path
   */
  public static void incrementUnreachable(@CompileTimeConstant String reason) {
    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      LOGGER.atError()
          .setCause(new Throwable("stacktrace"))
          .addKeyValue("reason", reason)
          .log("Invariant violated (this is a bug). "
              + "Please file a report with the attached stacktrace");
      return;
    }

    MeterRegistry registry = maybeRegistry.get();
    unreachableCounter(reason, registry).increment();
  }

  /**
   * Increments the counter for unloggable ID types.
   *
   * @param bsonType the BSON type that could not be logged
   */
  public static void incrementUnloggableIdType(String bsonType) {
    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      LOGGER
          .atWarn()
          .addKeyValue("bsonType", bsonType)
          .log("GlobalMetricFactory not initialized, cannot record unloggable ID type metric");
      return;
    }

    MeterRegistry registry = maybeRegistry.get();
    Counter.builder("mongot.unloggable_id_types")
        .description("Counts document IDs with types that cannot be logged")
        .tags(Tags.of("bsonType", bsonType))
        .register(registry)
        .increment();
  }

  /**
   * Records the duration of a detailed trace span into Prometheus-compatible Micrometer timers.
   *
   * <p>This is intentionally a silent no-op before metrics initialization because tracing classes
   * can be loaded before the community bootstrapper creates the registry.
   */
  public static void recordDetailedTraceSpan(String spanName, long durationNanos) {
    if (durationNanos < 0) {
      return;
    }

    Optional<MeterRegistry> maybeRegistry = meterRegistry;
    if (maybeRegistry.isEmpty()) {
      return;
    }

    Timer timer =
        detailedTraceSpanTimers.computeIfAbsent(
            spanName,
            ignored ->
                Timer.builder("trace.detailed.span.duration")
                    .description("Duration of detailed MongoT OpenTelemetry spans")
                    .tags(Tags.of("span", spanName, "timeUnit", "seconds"))
                    .publishPercentiles(0.5, 0.75, 0.9, 0.99)
                    .serviceLevelObjectives(DETAILED_TRACE_SPAN_BUCKETS)
                    .register(maybeRegistry.get()));
    timer.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  private static Duration micros(long micros) {
    return Duration.ofNanos(micros * 1_000);
  }

  private static Duration millis(long millis) {
    return Duration.ofMillis(millis);
  }

  @VisibleForTesting
  static void resetForTest() {
    meterRegistry = Optional.empty();
    detailedTraceSpanTimers.clear();
  }

  @VisibleForTesting
  static Optional<MeterRegistry> getRegistryForTest() {
    return meterRegistry;
  }
}
