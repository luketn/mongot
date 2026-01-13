package com.xgen.mongot.index.query.points;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Date;
import org.bson.BsonDateTime;
import org.bson.BsonValue;

public record DatePoint(Date value) implements Point, Comparable<DatePoint> {

  /** Deserializes a Point from BSON. */
  public static DatePoint fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return switch (value.getBsonType()) {
      case DATE_TIME -> new DatePoint(new Date(value.asDateTime().getValue()));
      default -> context.handleUnexpectedType("date", value.getBsonType());
    };
  }

  @Override
  public Type getType() {
    return Type.DATE;
  }

  @Override
  public int compareTo(DatePoint o) {
    return value().compareTo(o.value());
  }

  @Override
  public String toString() {
    return value().toString();
  }

  @Override
  public BsonValue toBson() {
    return new BsonDateTime(this.value.getTime());
  }
}
