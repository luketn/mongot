package com.xgen.testing.mongot.index.query.scores;

import com.xgen.mongot.index.query.scores.FunctionScore;
import com.xgen.mongot.index.query.scores.expressions.Expression;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class FunctionScoreBuilder {

  private Optional<Expression> expression = Optional.empty();

  public FunctionScoreBuilder expression(Expression expression) {
    this.expression = Optional.of(expression);
    return this;
  }

  /** Builds the FunctionScore. */
  public FunctionScore build() {
    Check.isPresent(this.expression, "expression");
    return new FunctionScore(this.expression.get());
  }
}
