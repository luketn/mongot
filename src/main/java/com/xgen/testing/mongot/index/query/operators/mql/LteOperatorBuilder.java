package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.LteOperator;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public class LteOperatorBuilder implements InequalityBuilder<LteOperator> {
  private Optional<Value> value = Optional.empty();

  @Override
  public LteOperatorBuilder value(Value value) {
    this.value = Optional.of(value);
    return this;
  }

  @Override
  public LteOperator build() throws BsonParseException {
    Check.isPresent(this.value, "value");
    return LteOperator.fromBson(BsonParseContext.root(), this.value.get().toBson());
  }
}
