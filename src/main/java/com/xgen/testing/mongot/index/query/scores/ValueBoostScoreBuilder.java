package com.xgen.testing.mongot.index.query.scores;

import com.xgen.mongot.index.query.scores.ValueBoostScore;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ValueBoostScoreBuilder {

  private Optional<Float> value = Optional.empty();

  public ValueBoostScoreBuilder value(float value) {
    this.value = Optional.of(value);
    return this;
  }

  /** Builds the BoostScore. */
  public ValueBoostScore build() {
    Check.isPresent(this.value, "value");
    return new ValueBoostScore(this.value.get());
  }
}
