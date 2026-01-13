package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.AddExpression;
import com.xgen.mongot.index.query.scores.expressions.Expression;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;

public class AddExpressionBuilder {

  private final List<Expression> expressions = new ArrayList<>();

  public static AddExpressionBuilder builder() {
    return new AddExpressionBuilder();
  }

  public AddExpressionBuilder arg(Expression expression) {
    this.expressions.add(expression);
    return this;
  }

  public AddExpression build() {
    Check.checkState(this.expressions.size() >= 2, "add expression must have at least 2 arguments");
    return new AddExpression(this.expressions);
  }
}
