package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.Expression;
import com.xgen.mongot.index.query.scores.expressions.Log1PExpression;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class Log1PExpressionBuilder {

  private Optional<Expression> expression = Optional.empty();

  public static Log1PExpressionBuilder builder() {
    return new Log1PExpressionBuilder();
  }

  public Log1PExpressionBuilder expression(Expression expression) {
    this.expression = Optional.of(expression);
    return this;
  }

  /** Builds the FunctionScore. */
  public Log1PExpression build() {
    Check.isPresent(this.expression, "expression");
    return new Log1PExpression(this.expression.get());
  }
}
