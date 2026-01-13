package com.xgen.mongot.index.query.points;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonBoolean;
import org.bson.BsonValue;

public record BooleanPoint(boolean value) implements Point, Comparable<BooleanPoint> {

  public static BooleanPoint fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return switch (value.getBsonType()) {
      case BOOLEAN -> new BooleanPoint(value.asBoolean().getValue());
      default -> context.handleUnexpectedType("boolean", value.getBsonType());
    };
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN;
  }

  @Override
  public int compareTo(BooleanPoint o) {
    return Boolean.compare(this.value, o.value());
  }

  @Override
  public BsonValue toBson() {
    return new BsonBoolean(this.value);
  }
}
