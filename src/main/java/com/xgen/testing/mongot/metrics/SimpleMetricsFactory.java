package com.xgen.testing.mongot.metrics;

import com.xgen.mongot.metrics.MetricsFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

/** A {@link MetricsFactory} initialized with a {@link SimpleMeterRegistry} */
public class SimpleMetricsFactory extends MetricsFactory {

  public final String namespace;
  public final MeterRegistry meterRegistry;

  public SimpleMetricsFactory() {
    this("DEFAULT-TEST", new SimpleMeterRegistry());
  }

  private SimpleMetricsFactory(String namespace, MeterRegistry meterRegistry) {
    super(namespace, meterRegistry);
    this.namespace = namespace;
    this.meterRegistry = meterRegistry;
  }
}
