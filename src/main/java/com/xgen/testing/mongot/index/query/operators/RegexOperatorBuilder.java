package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.RegexOperator;

public class RegexOperatorBuilder
    extends TermLevelOperatorBuilder<RegexOperator, RegexOperatorBuilder> {

  @Override
  RegexOperatorBuilder getBuilder() {
    return this;
  }

  @Override
  public RegexOperator build() {
    return new RegexOperator(getScore(), getPath(), getQuery(), getAllowAnalyzed());
  }
}
