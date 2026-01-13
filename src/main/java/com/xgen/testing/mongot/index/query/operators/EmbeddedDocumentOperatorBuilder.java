package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.EmbeddedDocumentOperator;
import com.xgen.mongot.index.query.operators.Operator;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import java.util.Optional;

public class EmbeddedDocumentOperatorBuilder
    extends OperatorBuilder<EmbeddedDocumentOperator, EmbeddedDocumentOperatorBuilder> {

  private Optional<FieldPath> path = Optional.empty();
  private Optional<Operator> operator = Optional.empty();

  public EmbeddedDocumentOperatorBuilder path(String path) {
    this.path = Optional.of(FieldPath.parse(path));
    return this;
  }

  public EmbeddedDocumentOperatorBuilder operator(Operator operator) {
    this.operator = Optional.of(operator);
    return this;
  }

  @Override
  public EmbeddedDocumentOperator build() {
    Check.isPresent(this.path, "path");
    Check.isPresent(this.operator, "operator");
    return new EmbeddedDocumentOperator(getScore(), this.path.get(), this.operator.get());
  }

  @Override
  EmbeddedDocumentOperatorBuilder getBuilder() {
    return this;
  }
}
