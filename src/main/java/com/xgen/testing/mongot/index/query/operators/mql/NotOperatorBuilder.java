package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.MqlFilterOperatorList;
import com.xgen.mongot.index.query.operators.mql.NotOperator;

public class NotOperatorBuilder implements MqlFilterOperatorBuilder<NotOperator> {
  private MqlFilterOperatorList value;

  public NotOperatorBuilder value(MqlFilterOperatorList value) {
    this.value = value;
    return this;
  }

  @Override
  public NotOperator build() {
    return new NotOperator(this.value);
  }
}
