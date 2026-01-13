package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.GteOperator;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public class GteOperatorBuilder implements InequalityBuilder<GteOperator> {
  private Optional<Value> value = Optional.empty();

  @Override
  public GteOperatorBuilder value(Value value) {
    this.value = Optional.of(value);
    return this;
  }

  @Override
  public GteOperator build() throws BsonParseException {
    Check.isPresent(this.value, "value");
    return GteOperator.fromBson(BsonParseContext.root(), this.value.get().toBson());
  }
}
