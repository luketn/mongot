package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.EqOperator;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class EqOperatorBuilder implements InequalityBuilder<EqOperator> {
  private Optional<Value> value = Optional.empty();

  @Override
  public EqOperatorBuilder value(Value value) {
    this.value = Optional.of(value);
    return this;
  }

  @Override
  public EqOperator build() {
    Check.isPresent(this.value, "value");
    return new EqOperator(this.value.get());
  }
}
