package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import org.bson.BsonBoolean;
import org.bson.BsonValue;

public record BooleanValue(boolean value) implements NonNullValue {

  @Override
  public ValueType getType() {
    return ValueType.BOOLEAN;
  }

  @Override
  public BsonValue toBson() {
    return new BsonBoolean(this.value);
  }

  @Override
  public NonNullValueType getNonNullType() {
    return NonNullValueType.BOOLEAN;
  }
}
