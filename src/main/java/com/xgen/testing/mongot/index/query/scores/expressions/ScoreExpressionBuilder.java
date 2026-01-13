package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.ScoreExpression;

public class ScoreExpressionBuilder {
  public static ScoreExpressionBuilder builder() {
    return new ScoreExpressionBuilder();
  }

  public ScoreExpression build() {
    return new ScoreExpression();
  }
}
