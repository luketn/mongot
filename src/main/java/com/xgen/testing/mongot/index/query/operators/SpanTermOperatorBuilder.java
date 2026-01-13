package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.SpanTermOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class SpanTermOperatorBuilder
    extends OperatorBuilder<SpanTermOperator, SpanTermOperatorBuilder> {

  private Optional<TermOperator> term = Optional.empty();

  @Override
  SpanTermOperatorBuilder getBuilder() {
    return this;
  }

  public SpanTermOperatorBuilder term(TermOperator term) {
    this.term = Optional.of(term);
    return this;
  }

  @Override
  public SpanTermOperator build() {
    Check.isPresent(this.term, "path");

    return new SpanTermOperator(getScore(), this.term.get());
  }
}
