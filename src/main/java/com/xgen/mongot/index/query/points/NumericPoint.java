package com.xgen.mongot.index.query.points;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonValue;

public sealed interface NumericPoint extends Point, Comparable<NumericPoint>
    permits DoublePoint, LongPoint {

  static NumericPoint fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return switch (value.getBsonType()) {
      case INT32 -> new LongPoint(value.asInt32().getValue());
      case INT64 -> new LongPoint(value.asInt64().getValue());
      case DOUBLE -> new DoublePoint(value.asDouble().getValue());
      default -> context.handleUnexpectedType("number", value.getBsonType());
    };
  }

  /**
   * Returns double storage-level value representation. See numeric-index-representation.md for
   * details.
   */
  long getDoubleValueRepresentation();

  /**
   * Returns long storage-level value representation. See numeric-index-representation.md for
   * details.
   */
  long getLongValueRepresentation();

  @Override
  default Point.Type getType() {
    return Point.Type.NUMBER;
  }
}
