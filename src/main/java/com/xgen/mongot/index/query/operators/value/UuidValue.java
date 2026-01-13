package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import java.util.UUID;
import org.bson.BsonBinary;
import org.bson.BsonValue;
import org.bson.UuidRepresentation;

public record UuidValue(UUID uuid) implements NonNullValue {

  @Override
  public ValueType getType() {
    return ValueType.UUID;
  }

  @Override
  public BsonValue toBson() {
    return new BsonBinary(this.uuid, UuidRepresentation.STANDARD);
  }

  @Override
  public NonNullValueType getNonNullType() {
    return NonNullValueType.UUID;
  }
}
