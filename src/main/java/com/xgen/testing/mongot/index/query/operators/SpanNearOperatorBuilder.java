package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.SpanNearOperator;
import com.xgen.mongot.index.query.operators.SpanOperator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SpanNearOperatorBuilder
    extends OperatorBuilder<SpanNearOperator, SpanNearOperatorBuilder> {

  private final List<SpanOperator> clauses = new ArrayList<>();
  private Optional<Boolean> inOrder = Optional.empty();
  private Optional<Integer> slop = Optional.empty();

  @Override
  SpanNearOperatorBuilder getBuilder() {
    return this;
  }

  public SpanNearOperatorBuilder clause(SpanOperator clause) {
    this.clauses.add(clause);
    return this;
  }

  public SpanNearOperatorBuilder inOrder(boolean inOrder) {
    this.inOrder = Optional.of(inOrder);
    return this;
  }

  public SpanNearOperatorBuilder slop(int slop) {
    this.slop = Optional.of(slop);
    return this;
  }

  @Override
  public SpanNearOperator build() {
    return new SpanNearOperator(
        getScore(),
        this.clauses,
        this.slop.orElse(SpanNearOperator.Fields.SLOP.getDefaultValue()),
        this.inOrder.orElse(SpanNearOperator.Fields.IN_ORDER.getDefaultValue()));
  }
}
