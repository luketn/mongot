package com.xgen.testing.mongot.index.query.scores;

public class ScoreBuilder {

  public static ValueBoostScoreBuilder valueBoost() {
    return new ValueBoostScoreBuilder();
  }

  public static PathBoostScoreBuilder pathBoost() {
    return new PathBoostScoreBuilder();
  }

  public static ConstantScoreBuilder constant() {
    return new ConstantScoreBuilder();
  }

  public static DismaxScoreBuilder dismax() {
    return new DismaxScoreBuilder();
  }

  public static FunctionScoreBuilder function() {
    return new FunctionScoreBuilder();
  }

  public static EmbeddedScoreBuilder embedded() {
    return new EmbeddedScoreBuilder();
  }
}
