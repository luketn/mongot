package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.Expression;
import com.xgen.mongot.index.query.scores.expressions.MultiplyExpression;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;

public class MultiplyExpressionBuilder {

  private final List<Expression> expressions = new ArrayList<>();

  public static MultiplyExpressionBuilder builder() {
    return new MultiplyExpressionBuilder();
  }

  public MultiplyExpressionBuilder arg(Expression expression) {
    this.expressions.add(expression);
    return this;
  }

  /** Builds the MultiplyExpression. */
  public MultiplyExpression build() {
    Check.checkState(
        this.expressions.size() >= 2, "multiply expression must have at least 2 arguments");
    return new MultiplyExpression(this.expressions);
  }
}
