package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.index.query.operators.SpanOrOperator;
import java.util.ArrayList;
import java.util.List;

public class SpanOrOperatorBuilder extends OperatorBuilder<SpanOrOperator, SpanOrOperatorBuilder> {

  private final List<SpanOperator> clauses = new ArrayList<>();

  @Override
  SpanOrOperatorBuilder getBuilder() {
    return this;
  }

  public SpanOrOperatorBuilder clause(SpanOperator clause) {
    this.clauses.add(clause);
    return this;
  }

  @Override
  public SpanOrOperator build() {
    return new SpanOrOperator(getScore(), this.clauses);
  }
}
