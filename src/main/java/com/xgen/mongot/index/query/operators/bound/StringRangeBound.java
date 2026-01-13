package com.xgen.mongot.index.query.operators.bound;

import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public final class StringRangeBound extends RangeBound<StringPoint> {

  public StringRangeBound(
      Optional<StringPoint> lower,
      Optional<StringPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    super(lower, upper, lowerInclusive, upperInclusive);
  }

  public static StringRangeBound create(
      BsonParseContext context,
      Optional<StringPoint> lower,
      Optional<StringPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    RangeBound.validate(context, lower, upper, lowerInclusive, upperInclusive);
    return new StringRangeBound(lower, upper, lowerInclusive, upperInclusive);
  }

  @Override
  public ValueType getType() {
    return ValueType.STRING;
  }
}
