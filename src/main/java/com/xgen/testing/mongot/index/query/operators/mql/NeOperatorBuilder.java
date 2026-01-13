package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.NeOperator;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class NeOperatorBuilder implements InequalityBuilder<NeOperator> {
  private Optional<Value> value = Optional.empty();

  @Override
  public NeOperatorBuilder value(Value value) {
    this.value = Optional.of(value);
    return this;
  }

  @Override
  public NeOperator build() {
    Check.isPresent(this.value, "value");
    return new NeOperator(this.value.get());
  }
}
