package com.xgen.testing.mongot.metrics.micrometer;

import com.xgen.mongot.metrics.micrometer.Percentiles;
import com.xgen.mongot.metrics.micrometer.SerializableDistributionSummary;
import java.util.Optional;

public class SerializableDistributionSummaryBuilder {

  private long count = 0L;
  private double total = 0.0;
  private double max = 0.0;
  private double mean = 0.0;
  private Optional<Percentiles> percentiles = Optional.empty();

  public static SerializableDistributionSummaryBuilder builder() {
    return new SerializableDistributionSummaryBuilder();
  }

  public SerializableDistributionSummaryBuilder count(long count) {
    this.count = count;
    return this;
  }

  public SerializableDistributionSummaryBuilder total(double total) {
    this.total = total;
    return this;
  }

  public SerializableDistributionSummaryBuilder max(double max) {
    this.max = max;
    return this;
  }

  public SerializableDistributionSummaryBuilder mean(double mean) {
    this.mean = mean;
    return this;
  }

  public SerializableDistributionSummaryBuilder percentiles(Percentiles percentiles) {
    this.percentiles = Optional.of(percentiles);
    return this;
  }

  public SerializableDistributionSummary build() {
    return new SerializableDistributionSummary(
        this.count, this.total, this.max, this.mean, this.percentiles);
  }
}
