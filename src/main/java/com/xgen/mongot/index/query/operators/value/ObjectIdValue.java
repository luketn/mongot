package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

public record ObjectIdValue(ObjectId value) implements NonNullValue {

  @Override
  public ValueType getType() {
    return ValueType.OBJECT_ID;
  }

  @Override
  public BsonValue toBson() {
    return new BsonObjectId(this.value);
  }

  @Override
  public NonNullValueType getNonNullType() {
    return NonNullValueType.OBJECT_ID;
  }
}
