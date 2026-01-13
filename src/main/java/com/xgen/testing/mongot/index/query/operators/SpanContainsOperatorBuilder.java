package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.SpanContainsOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class SpanContainsOperatorBuilder
    extends OperatorBuilder<SpanContainsOperator, SpanContainsOperatorBuilder> {

  private Optional<SpanOperator> big = Optional.empty();
  private Optional<SpanOperator> little = Optional.empty();
  private Optional<SpanContainsOperator.SpanToReturn> spanToReturn = Optional.empty();

  @Override
  SpanContainsOperatorBuilder getBuilder() {
    return this;
  }

  public SpanContainsOperatorBuilder big(SpanOperator big) {
    this.big = Optional.of(big);
    return this;
  }

  public SpanContainsOperatorBuilder little(SpanOperator little) {
    this.little = Optional.of(little);
    return this;
  }

  public SpanContainsOperatorBuilder spanToReturn(SpanContainsOperator.SpanToReturn spanToReturn) {
    this.spanToReturn = Optional.of(spanToReturn);
    return this;
  }

  @Override
  public SpanContainsOperator build() {
    Check.isPresent(this.big, "big");
    Check.isPresent(this.little, "little");
    Check.isPresent(this.spanToReturn, "spanToReturn");

    return new SpanContainsOperator(
        getScore(), this.little.get(), this.big.get(), this.spanToReturn.get());
  }
}
