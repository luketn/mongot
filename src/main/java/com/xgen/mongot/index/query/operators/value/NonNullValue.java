package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.type.NonNullValueType;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonValue;

public sealed interface NonNullValue extends Value
    permits BooleanValue, DateValue, NumericValue, ObjectIdValue, StringValue, UuidValue {
  NonNullValueType getNonNullType();

  static NonNullValue fromBson(BsonParseContext context, BsonValue bsonValue)
      throws BsonParseException {
    Value value = Value.fromBson(context, bsonValue);
    if (value.getType() == ValueType.NULL) {
      return context.handleSemanticError("value type cannot be null");
    }
    return (NonNullValue) value;
  }
}
