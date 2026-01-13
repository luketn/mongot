package com.xgen.mongot.index.query.operators.bound;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public final class NumericRangeBound extends RangeBound<NumericPoint> {

  public NumericRangeBound(
      Optional<NumericPoint> lower,
      Optional<NumericPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    super(lower, upper, lowerInclusive, upperInclusive);
  }

  public static NumericRangeBound create(
      BsonParseContext context,
      Optional<NumericPoint> lower,
      Optional<NumericPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    RangeBound.validate(context, lower, upper, lowerInclusive, upperInclusive);
    return new NumericRangeBound(lower, upper, lowerInclusive, upperInclusive);
  }

  @Override
  public ValueType getType() {
    return ValueType.NUMBER;
  }
}
