package com.xgen.mongot.index.query.operators.value;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.Encodable;
import java.util.Collection;
import java.util.Date;
import org.bson.BsonBinarySubType;
import org.bson.BsonValue;

public sealed interface Value extends Encodable permits NonNullValue, NullValue {

  ValueType getType();

  /** Deserializes a Value from BSON. */
  static Value fromBson(BsonParseContext context, BsonValue value) throws BsonParseException {
    switch (value.getBsonType()) {
      case BOOLEAN -> {
        return new BooleanValue(value.asBoolean().getValue());
      }
      case OBJECT_ID -> {
        return new ObjectIdValue(value.asObjectId().getValue());
      }
      case INT32 -> {
        return new NumericValue(new LongPoint((long) value.asInt32().getValue()));
      }
      case INT64 -> {
        return new NumericValue(new LongPoint(value.asInt64().getValue()));
      }
      case DOUBLE -> {
        return new NumericValue(new DoublePoint(value.asDouble().getValue()));
      }
      case DATE_TIME -> {
        return new DateValue(new DatePoint(new Date(value.asDateTime().getValue())));
      }
      case STRING -> {
        return new StringValue(value.asString().getValue());
      }
      case BINARY -> {
        byte currentBinarySubType = value.asBinary().getType();
        if (currentBinarySubType != BsonBinarySubType.UUID_STANDARD.getValue()) {
          context.handleSemanticError("binary value must be UUID subtype 4");
        }
        return new UuidValue(value.asBinary().asUuid());
      }
      case NULL -> {
        return new NullValue();
      }
      default -> {
        return context.handleUnexpectedType(
            "boolean, objectId, number, string, date, uuid, or null", value.getBsonType());
      }
    }
  }

  static boolean containsDistinctType(Collection<? extends Value> values) {
    return values.stream().map(Value::getType).distinct().count() > 1;
  }

  @Override
  BsonValue toBson();

  default BooleanValue asBoolean() {
    Check.expectedType(ValueType.BOOLEAN, getType());
    return (BooleanValue) this;
  }

  default ObjectIdValue asObjectId() {
    Check.expectedType(ValueType.OBJECT_ID, getType());
    return (ObjectIdValue) this;
  }

  default NumericValue asNumber() {
    Check.expectedType(ValueType.NUMBER, getType());
    return (NumericValue) this;
  }

  default DateValue asDate() {
    Check.expectedType(ValueType.DATE, getType());
    return (DateValue) this;
  }

  default StringValue asString() {
    Check.expectedType(ValueType.STRING, getType());
    return (StringValue) this;
  }

  default UuidValue asUuid() {
    Check.expectedType(ValueType.UUID, getType());
    return (UuidValue) this;
  }
}
