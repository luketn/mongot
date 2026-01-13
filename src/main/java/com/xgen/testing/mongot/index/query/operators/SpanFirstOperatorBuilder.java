package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.SpanFirstOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class SpanFirstOperatorBuilder
    extends OperatorBuilder<SpanFirstOperator, SpanFirstOperatorBuilder> {

  private Optional<SpanOperator> operator = Optional.empty();
  private Optional<Integer> endPositionLte = Optional.empty();

  @Override
  SpanFirstOperatorBuilder getBuilder() {
    return this;
  }

  public SpanFirstOperatorBuilder operator(SpanOperator operator) {
    this.operator = Optional.of(operator);
    return this;
  }

  public SpanFirstOperatorBuilder endPositionLte(int endPositionLte) {
    this.endPositionLte = Optional.of(endPositionLte);
    return this;
  }

  @Override
  public SpanFirstOperator build() {
    Check.isPresent(this.operator, "operator");

    return new SpanFirstOperator(
        getScore(),
        this.operator.get(),
        this.endPositionLte.orElse(SpanFirstOperator.Fields.END_POSITION_LTE.getDefaultValue()));
  }
}
