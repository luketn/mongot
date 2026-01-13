package com.xgen.mongot.index.query.operators.bound;

import com.google.common.base.Objects;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.Optional;

public abstract sealed class RangeBound<P extends Point & Comparable<P>>
    permits BooleanRangeBound,
        DateRangeBound,
        NumericRangeBound,
        ObjectIdRangeBound,
        StringRangeBound,
        UuidRangeBound {

  private final Optional<P> lower;
  private final Optional<P> upper;
  private final boolean lowerInclusive;
  private final boolean upperInclusive;

  RangeBound(Optional<P> lower, Optional<P> upper, boolean lowerInclusive, boolean upperInclusive) {
    this.lower = lower;
    this.upper = upper;
    this.lowerInclusive = lowerInclusive;
    this.upperInclusive = upperInclusive;
  }

  static <P extends Comparable<P>> void validate(
      BsonParseContext context,
      Optional<P> lower,
      Optional<P> upper,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {
    if (upper.isPresent() && lower.isPresent()) {
      if (lower.get().compareTo(upper.get()) > 0) {
        context.handleSemanticError("gt/gte must not be greater than lt/lte");
      }

      if (lower.get().compareTo(upper.get()) == 0 && (!lowerInclusive || !upperInclusive)) {
        if (lower.get().equals(new DoublePoint(Double.NaN))
            != upper.get().equals(new DoublePoint(Double.NaN))) {
          // If exactly one of these is NaN, that means compareTo gave a false equality.
          return;
        }
        context.handleSemanticError("bounds must both be inclusive if they are equal");
      }
    }
  }

  public abstract ValueType getType();

  public Optional<P> getLower() {
    return this.lower;
  }

  public Optional<P> getUpper() {
    return this.upper;
  }

  public boolean upperInclusive() {
    return this.upperInclusive;
  }

  public boolean lowerInclusive() {
    return this.lowerInclusive;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RangeBound<?> that = (RangeBound<?>) o;
    return this.lowerInclusive == that.lowerInclusive
        && this.upperInclusive == that.upperInclusive
        && Objects.equal(this.lower, that.lower)
        && Objects.equal(this.upper, that.upper);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.lower, this.upper, this.lowerInclusive, this.upperInclusive);
  }

  @Override
  public String toString() {
    return String.format(
        "%s%s, %s%s",
        this.lowerInclusive() ? "[" : "(",
        this.getLower().map(Object::toString).orElse("MIN_RANGE_VALUE"),
        this.getUpper().map(Object::toString).orElse("MAX_RANGE_VALUE"),
        this.upperInclusive() ? "]" : ")");
  }
}
