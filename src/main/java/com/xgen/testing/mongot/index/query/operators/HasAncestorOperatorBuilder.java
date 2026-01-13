package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.HasAncestorOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class HasAncestorOperatorBuilder
    extends OperatorBuilder<HasAncestorOperator, HasAncestorOperatorBuilder> {

  private Optional<Operator> operator = Optional.empty();
  private Optional<FieldPath> ancestorPath = Optional.empty();

  public <T extends Operator> HasAncestorOperatorBuilder operator(T operator) {
    this.operator = Optional.of(operator);
    return this;
  }

  public HasAncestorOperatorBuilder ancestorPath(String path) {
    this.ancestorPath = Optional.of(FieldPath.parse(path));
    return this;
  }

  @Override
  protected HasAncestorOperatorBuilder getBuilder() {
    return this;
  }

  @Override
  public HasAncestorOperator build() {
    return new HasAncestorOperator(
        getScore(), this.ancestorPath.orElseThrow(), this.operator.orElseThrow());
  }
}
