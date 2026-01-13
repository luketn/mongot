package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.WildcardOperator;

public class WildcardOperatorBuilder
    extends TermLevelOperatorBuilder<WildcardOperator, WildcardOperatorBuilder> {

  @Override
  WildcardOperatorBuilder getBuilder() {
    return this;
  }

  @Override
  public WildcardOperator build() {
    return new WildcardOperator(getScore(), getPath(), getQuery(), getAllowAnalyzed());
  }
}
