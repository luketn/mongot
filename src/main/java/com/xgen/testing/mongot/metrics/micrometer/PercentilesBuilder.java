package com.xgen.testing.mongot.metrics.micrometer;

import com.xgen.mongot.metrics.micrometer.Percentiles;
import java.util.Optional;

public class PercentilesBuilder {
  private Optional<Double> percentile50 = Optional.empty();
  private Optional<Double> percentile75 = Optional.empty();
  private Optional<Double> percentile90 = Optional.empty();
  private Optional<Double> percentile99 = Optional.empty();

  public static PercentilesBuilder builder() {
    return new PercentilesBuilder();
  }

  public PercentilesBuilder percentile50(double percentile50) {
    this.percentile50 = Optional.of(percentile50);
    return this;
  }

  public PercentilesBuilder percentile75(double percentile75) {
    this.percentile75 = Optional.of(percentile75);
    return this;
  }

  public PercentilesBuilder percentile90(double percentile90) {
    this.percentile90 = Optional.of(percentile90);
    return this;
  }

  public PercentilesBuilder percentile99(double percentile99) {
    this.percentile99 = Optional.of(percentile99);
    return this;
  }

  public PercentilesBuilder zeroPercentiles() {
    this.percentile50 = Optional.of(0.0);
    this.percentile75 = Optional.of(0.0);
    this.percentile90 = Optional.of(0.0);
    this.percentile99 = Optional.of(0.0);
    return this;
  }

  public Percentiles build() {
    return new Percentiles(
        Optional.empty(),
        this.percentile50,
        this.percentile75,
        this.percentile90,
        this.percentile99);
  }
}
