package com.xgen.testing.mongot.index.query.operators.mql;

import com.xgen.mongot.index.query.operators.mql.GtOperator;
import com.xgen.mongot.index.query.operators.value.Value;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public class GtOperatorBuilder implements InequalityBuilder<GtOperator> {
  private Optional<Value> value = Optional.empty();

  @Override
  public GtOperatorBuilder value(Value value) {
    this.value = Optional.of(value);
    return this;
  }

  @Override
  public GtOperator build() throws BsonParseException {
    Check.isPresent(this.value, "value");
    return GtOperator.fromBson(BsonParseContext.root(), this.value.get().toBson());
  }
}
