package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.HasRootOperator;
import com.xgen.mongot.index.query.operators.Operator;
import java.util.Optional;

public class HasRootOperatorBuilder
    extends OperatorBuilder<HasRootOperator, HasRootOperatorBuilder> {

  private Optional<Operator> operator = Optional.empty();

  public <T extends Operator> HasRootOperatorBuilder operator(T operator) {
    this.operator = Optional.of(operator);
    return this;
  }

  @Override
  protected HasRootOperatorBuilder getBuilder() {
    return this;
  }

  @Override
  public HasRootOperator build() {
    return new HasRootOperator(getScore(), this.operator.orElseThrow());
  }
}
