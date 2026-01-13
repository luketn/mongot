package com.xgen.testing.mongot.index.query.scores;

import com.xgen.mongot.index.query.scores.PathBoostScore;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class PathBoostScoreBuilder {

  private Optional<FieldPath> path = Optional.empty();
  private Optional<Double> undefined = Optional.empty();

  public PathBoostScoreBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public PathBoostScoreBuilder undefined(double undefined) {
    this.undefined = Optional.of(undefined);
    return this;
  }

  /** Builds the BoostScore. */
  public PathBoostScore build() {
    Check.isPresent(this.path, "path");
    return new PathBoostScore(this.path.get(), this.undefined.orElse(0d));
  }
}
