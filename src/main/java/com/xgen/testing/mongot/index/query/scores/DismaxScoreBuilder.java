package com.xgen.testing.mongot.index.query.scores;

import com.xgen.mongot.index.query.scores.DismaxScore;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class DismaxScoreBuilder {

  private Optional<Float> tieBreakerScore = Optional.empty();

  public DismaxScoreBuilder tieBreakerScore(float tieBreakerScore) {
    this.tieBreakerScore = Optional.of(tieBreakerScore);
    return this;
  }

  /** Builds the DismaxScore. */
  public DismaxScore build() {
    Check.isPresent(this.tieBreakerScore, "tieBreakerScore");

    return new DismaxScore(this.tieBreakerScore.get());
  }
}
