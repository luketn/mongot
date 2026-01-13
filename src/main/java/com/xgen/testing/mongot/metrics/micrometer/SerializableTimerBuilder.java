package com.xgen.testing.mongot.metrics.micrometer;

import com.xgen.mongot.metrics.micrometer.Percentiles;
import com.xgen.mongot.metrics.micrometer.SerializableTimer;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class SerializableTimerBuilder {

  private TimeUnit timeUnit = TimeUnit.SECONDS;
  private long count = 0L;
  private double totalTime = 0.0;
  private double max = 0.0;
  private double mean = 0.0;
  private Optional<Percentiles> percentiles = Optional.empty();

  public static SerializableTimerBuilder builder() {
    return new SerializableTimerBuilder();
  }

  public SerializableTimerBuilder timeUnit(TimeUnit timeUnit) {
    this.timeUnit = timeUnit;
    return this;
  }

  public SerializableTimerBuilder count(long count) {
    this.count = count;
    return this;
  }

  public SerializableTimerBuilder totalTime(double totalTime) {
    this.totalTime = totalTime;
    return this;
  }

  public SerializableTimerBuilder max(double max) {
    this.max = max;
    return this;
  }

  public SerializableTimerBuilder mean(double mean) {
    this.mean = mean;
    return this;
  }

  public SerializableTimerBuilder percentiles(Percentiles percentiles) {
    this.percentiles = Optional.of(percentiles);
    return this;
  }

  public SerializableTimer build() {
    return new SerializableTimer(
        this.timeUnit, this.count, this.totalTime, this.max, this.mean, this.percentiles);
  }
}
