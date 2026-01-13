package com.xgen.testing.mongot.index.query.operators.mql;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.index.query.operators.mql.NinOperator;
import com.xgen.mongot.index.query.operators.value.NonNullValue;
import java.util.ArrayList;
import java.util.List;

public class NinOperatorBuilder implements ListInequalityBuilder<NinOperatorBuilder> {
  private List<NonNullValue> values = new ArrayList<>();

  @Override
  public NinOperatorBuilder values(List<NonNullValue> values) {
    this.values = values;
    return this;
  }

  @Override
  public NinOperatorBuilder addValue(NonNullValue value) {
    this.values.add(value);
    return this;
  }

  public NinOperator build() {
    checkArg(!this.values.isEmpty(), "$nin must have at least 1 value");
    return new NinOperator(this.values);
  }
}
