package com.xgen.mongot.index.query.points;

import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

public record ObjectIdPoint(ObjectId value) implements Point, Comparable<ObjectIdPoint> {

  public static ObjectIdPoint fromBson(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    return switch (value.getBsonType()) {
      case OBJECT_ID -> new ObjectIdPoint(value.asObjectId().getValue());
      default -> context.handleUnexpectedType("objectId", value.getBsonType());
    };
  }

  @Override
  public Type getType() {
    return Type.OBJECT_ID;
  }

  @Override
  public int compareTo(ObjectIdPoint o) {
    return this.value.compareTo(o.value());
  }

  @Override
  public BsonValue toBson() {
    return new BsonObjectId(this.value);
  }
}
