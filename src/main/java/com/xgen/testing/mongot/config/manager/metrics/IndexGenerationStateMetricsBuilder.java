package com.xgen.testing.mongot.config.manager.metrics;

import com.xgen.mongot.config.manager.metrics.IndexConfigState;
import com.xgen.mongot.config.manager.metrics.IndexGenerationStateMetrics;
import com.xgen.mongot.index.IndexGenerationMetrics;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class IndexGenerationStateMetricsBuilder {
  public Optional<IndexConfigState> state = Optional.empty();
  public Optional<IndexGenerationMetrics> metrics = Optional.empty();

  public static IndexGenerationStateMetricsBuilder builder() {
    return new IndexGenerationStateMetricsBuilder();
  }

  public IndexGenerationStateMetricsBuilder state(IndexConfigState state) {
    this.state = Optional.of(state);
    return this;
  }

  public IndexGenerationStateMetricsBuilder indexGenerationMetrics(IndexGenerationMetrics metrics) {
    this.metrics = Optional.of(metrics);
    return this;
  }

  public IndexGenerationStateMetrics build() {
    Check.isPresent(this.state, "state");
    Check.isPresent(this.metrics, "metrics");

    return new IndexGenerationStateMetrics(this.metrics.get(), this.state.get());
  }
}
