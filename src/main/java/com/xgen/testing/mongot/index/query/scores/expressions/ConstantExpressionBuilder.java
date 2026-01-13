package com.xgen.testing.mongot.index.query.scores.expressions;

import com.xgen.mongot.index.query.scores.expressions.ConstantExpression;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ConstantExpressionBuilder {

  private Optional<Double> constant;

  public static ConstantExpressionBuilder builder() {
    return new ConstantExpressionBuilder();
  }

  public ConstantExpressionBuilder constant(double constant) {
    this.constant = Optional.of(constant);
    return this;
  }

  public ConstantExpression build() {
    Check.isPresent(this.constant, "constant");
    return new ConstantExpression(this.constant.get());
  }
}
