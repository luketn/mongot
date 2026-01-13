package com.xgen.testing.mongot.index.query.scores;

import com.xgen.mongot.index.query.scores.EmbeddedScore;
import com.xgen.mongot.index.query.scores.EmbeddedScore.Aggregate;
import com.xgen.mongot.index.query.scores.Score;
import java.util.Optional;

public class EmbeddedScoreBuilder {

  private Optional<Aggregate> aggregate = Optional.empty();
  private Optional<Score> outerScore = Optional.empty();

  public EmbeddedScoreBuilder aggregate(Aggregate aggregate) {
    this.aggregate = Optional.of(aggregate);
    return this;
  }

  public EmbeddedScoreBuilder outerScore(Score outerScore) {
    this.outerScore = Optional.of(outerScore);
    return this;
  }

  public EmbeddedScore build() {
    return new EmbeddedScore(
        this.aggregate.orElse(Aggregate.SUM), this.outerScore.orElse(Score.defaultScore()));
  }
}
