package com.xgen.testing.mongot.index.query.scores;

import com.xgen.mongot.index.query.scores.ConstantScore;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ConstantScoreBuilder {

  private Optional<Float> value = Optional.empty();

  public ConstantScoreBuilder value(float value) {
    this.value = Optional.of(value);
    return this;
  }

  /** Builds the ConstantScore. */
  public ConstantScore build() {
    Check.isPresent(this.value, "value");

    return new ConstantScore(this.value.get());
  }
}
