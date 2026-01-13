package com.xgen.testing.mongot.index.query.operators.mql;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.query.operators.mql.InOperator;
import com.xgen.mongot.index.query.operators.value.NonNullValue;
import java.util.ArrayList;
import java.util.List;

public class InOperatorBuilder implements ListInequalityBuilder<InOperatorBuilder> {
  private List<NonNullValue> values = new ArrayList<>();

  @Override
  public InOperatorBuilder values(List<NonNullValue> values) {
    this.values = values;
    return this;
  }

  @Override
  public InOperatorBuilder addValue(NonNullValue value) {
    this.values.add(value);
    return this;
  }

  public InOperator build() {
    checkArg(!this.values.isEmpty(), "$in must have at least 1 value");
    return new InOperator(this.values);
  }
}
