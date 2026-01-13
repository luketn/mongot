package com.xgen.mongot.index.query.operators.bound;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.BooleanPoint;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public final class BooleanRangeBound extends RangeBound<BooleanPoint> {

  public BooleanRangeBound(
      Optional<BooleanPoint> lower,
      Optional<BooleanPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    super(lower, upper, lowerInclusive, upperInclusive);
  }

  public static BooleanRangeBound create(
      BsonParseContext context,
      Optional<BooleanPoint> lower,
      Optional<BooleanPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    RangeBound.validate(context, lower, upper, lowerInclusive, upperInclusive);
    return new BooleanRangeBound(lower, upper, lowerInclusive, upperInclusive);
  }

  @Override
  public ValueType getType() {
    return ValueType.BOOLEAN;
  }
}
