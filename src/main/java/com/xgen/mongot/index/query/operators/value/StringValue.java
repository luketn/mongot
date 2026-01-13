package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.StringPoint;
import org.bson.BsonString;
import org.bson.BsonValue;

public record StringValue(String value) implements NonNullValue {

  @Override
  public ValueType getType() {
    return ValueType.STRING;
  }

  @Override
  public BsonValue toBson() {
    return new BsonString(this.value);
  }

  @Override
  public NonNullValueType getNonNullType() {
    return NonNullValueType.STRING;
  }

  public StringPoint getPoint() {
    return new StringPoint(this.value);
  }
}
