package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.type.ValueType;
import org.bson.BsonNull;
import org.bson.BsonValue;

public record NullValue() implements Value {
  @Override
  public BsonValue toBson() {
    return BsonNull.VALUE;
  }

  @Override
  public ValueType getType() {
    return ValueType.NULL;
  }
}
