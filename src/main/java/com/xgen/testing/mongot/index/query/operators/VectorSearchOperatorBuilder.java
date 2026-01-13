package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.VectorSearchCriteria;
import com.xgen.mongot.index.query.operators.VectorSearchOperator;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class VectorSearchOperatorBuilder
    extends OperatorBuilder<VectorSearchOperator, VectorSearchOperatorBuilder> {

  private Optional<VectorSearchCriteria> criteria = Optional.empty();

  @Override
  protected VectorSearchOperatorBuilder getBuilder() {
    return this;
  }

  public VectorSearchOperatorBuilder criteria(VectorSearchCriteria criteria) {
    this.criteria = Optional.of(criteria);
    return this;
  }

  @Override
  public VectorSearchOperator build() {
    VectorSearchCriteria criteria = Check.isPresent(this.criteria, "criteria");
    return new VectorSearchOperator(getScore(), criteria);
  }
}
