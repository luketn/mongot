package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.ExistsOperator;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ExistsOperatorBuilder extends OperatorBuilder<ExistsOperator, ExistsOperatorBuilder> {

  private Optional<String> path = Optional.empty();

  @Override
  protected ExistsOperatorBuilder getBuilder() {
    return this;
  }

  public ExistsOperatorBuilder path(String path) {
    this.path = Optional.of(path);
    return this;
  }

  @Override
  public ExistsOperator build() {
    Check.isPresent(this.path, "path");

    return new ExistsOperator(getScore(), this.path.get());
  }
}
