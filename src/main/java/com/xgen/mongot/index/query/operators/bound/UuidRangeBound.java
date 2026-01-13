package com.xgen.mongot.index.query.operators.bound;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.UuidPoint;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public final class UuidRangeBound extends RangeBound<UuidPoint> {

  public UuidRangeBound(
      Optional<UuidPoint> lower,
      Optional<UuidPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    super(lower, upper, lowerInclusive, upperInclusive);
  }

  public static UuidRangeBound create(
      BsonParseContext context,
      Optional<UuidPoint> lower,
      Optional<UuidPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    RangeBound.validate(context, lower, upper, lowerInclusive, upperInclusive);
    return new UuidRangeBound(lower, upper, lowerInclusive, upperInclusive);
  }

  @Override
  public ValueType getType() {
    return ValueType.UUID;
  }
}
