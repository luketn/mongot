package com.xgen.testing.mongot.index.query.counts;

import com.xgen.mongot.index.query.counts.Count;
import java.util.Optional;

public class CountBuilder {

  private Optional<Count.Type> type = Optional.empty();
  private Optional<Integer> threshold = Optional.empty();

  public static CountBuilder builder() {
    return new CountBuilder();
  }

  public CountBuilder type(Count.Type type) {
    this.type = Optional.of(type);
    return this;
  }

  public CountBuilder threshold(Integer threshold) {
    this.threshold = Optional.of(threshold);
    return this;
  }

  /** Build the Count. */
  public Count build() {
    return new Count(
        this.type.orElse(Count.DEFAULT.type()), this.threshold.orElse(Count.DEFAULT.threshold()));
  }
}
