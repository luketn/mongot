package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.ExistsOperator;

public class ExistsOperatorBuilder implements MqlFilterOperatorBuilder<ExistsOperator> {
  private boolean value;

  public ExistsOperatorBuilder value(boolean value) {
    this.value = value;
    return this;
  }

  @Override
  public ExistsOperator build() {
    return new ExistsOperator(this.value);
  }
}
