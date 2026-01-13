package com.xgen.mongot.index.query.operators.bound;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public final class DateRangeBound extends RangeBound<DatePoint> {

  public DateRangeBound(
      Optional<DatePoint> lower,
      Optional<DatePoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    super(lower, upper, lowerInclusive, upperInclusive);
  }

  public static DateRangeBound create(
      BsonParseContext context,
      Optional<DatePoint> lower,
      Optional<DatePoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    RangeBound.validate(context, lower, upper, lowerInclusive, upperInclusive);
    return new DateRangeBound(lower, upper, lowerInclusive, upperInclusive);
  }

  @Override
  public ValueType getType() {
    return ValueType.DATE;
  }
}
