package com.xgen.mongot.metrics;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;

public class Instrumentation {
  private Instrumentation() {}

  /** Registers built-in micrometer metrics on the supplied meterRegistry. */
  public static void instrumentJvmMetrics(MeterRegistry meterRegistry) {
    Tag jvmTag = ServerStatusDataExtractor.Scope.JVM.getTag();

    new JvmGcMetrics(Tags.of(jvmTag)).bindTo(meterRegistry);
    new JvmMemoryMetrics(Tags.of(jvmTag)).bindTo(meterRegistry);
    new UptimeMetrics(Tags.of(jvmTag)).bindTo(meterRegistry);

    // The following metrics are not reported to ServerStatus
    new ProcessorMetrics(Tags.of(jvmTag)).bindTo(meterRegistry);
  }
}
