package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.Expression;
import com.xgen.mongot.index.query.scores.expressions.LogExpression;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class LogExpressionBuilder {

  private Optional<Expression> expression = Optional.empty();

  public static LogExpressionBuilder builder() {
    return new LogExpressionBuilder();
  }

  public LogExpressionBuilder expression(Expression expression) {
    this.expression = Optional.of(expression);
    return this;
  }

  /** Builds the FunctionScore. */
  public LogExpression build() {
    Check.isPresent(this.expression, "expression");
    return new LogExpression(this.expression.get());
  }
}
