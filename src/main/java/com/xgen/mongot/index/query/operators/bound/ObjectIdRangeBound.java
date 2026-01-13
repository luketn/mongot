package com.xgen.mongot.index.query.operators.bound;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.ObjectIdPoint;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public final class ObjectIdRangeBound extends RangeBound<ObjectIdPoint> {

  public ObjectIdRangeBound(
      Optional<ObjectIdPoint> lower,
      Optional<ObjectIdPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    super(lower, upper, lowerInclusive, upperInclusive);
  }

  public static ObjectIdRangeBound create(
      BsonParseContext context,
      Optional<ObjectIdPoint> lower,
      Optional<ObjectIdPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    RangeBound.validate(context, lower, upper, lowerInclusive, upperInclusive);
    return new ObjectIdRangeBound(lower, upper, lowerInclusive, upperInclusive);
  }

  @Override
  public ValueType getType() {
    return ValueType.OBJECT_ID;
  }
}
