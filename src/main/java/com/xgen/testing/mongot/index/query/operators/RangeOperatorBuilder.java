package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.RangeOperator;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.ObjectIdRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.operators.bound.UuidRangeBound;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.index.query.points.ObjectIdPoint;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.index.query.points.UuidPoint;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.List;
import java.util.Optional;

public class RangeOperatorBuilder extends PathOperatorBuilder<RangeOperator, RangeOperatorBuilder> {

  private Optional<RangeBound<? extends Point>> range;
  private Optional<List<String>> doesNotAffect = Optional.empty();

  @Override
  protected RangeOperatorBuilder getBuilder() {
    return this;
  }

  public RangeOperatorBuilder dateBounds(
      Optional<DatePoint> lower,
      Optional<DatePoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    try {
      this.range =
          Optional.of(
              DateRangeBound.create(
                  BsonParseContext.root(), lower, upper, lowerInclusive, upperInclusive));
      return this;
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public RangeOperatorBuilder numericBounds(
      Optional<NumericPoint> lower,
      Optional<NumericPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    try {
      this.range =
          Optional.of(
              NumericRangeBound.create(
                  BsonParseContext.root(), lower, upper, lowerInclusive, upperInclusive));
      return this;
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public RangeOperatorBuilder objectIdBounds(
      Optional<ObjectIdPoint> lower,
      Optional<ObjectIdPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    try {
      this.range =
          Optional.of(
              ObjectIdRangeBound.create(
                  BsonParseContext.root(), lower, upper, lowerInclusive, upperInclusive));
      return this;
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public RangeOperatorBuilder uuidBounds(
      Optional<UuidPoint> lower,
      Optional<UuidPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    try {
      this.range =
          Optional.of(
              UuidRangeBound.create(
                  BsonParseContext.root(), lower, upper, lowerInclusive, upperInclusive));
      return this;
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public RangeOperatorBuilder stringBounds(
      Optional<StringPoint> lower,
      Optional<StringPoint> upper,
      boolean lowerInclusive,
      boolean upperInclusive) {
    try {
      this.range =
          Optional.of(
              StringRangeBound.create(
                  BsonParseContext.root(), lower, upper, lowerInclusive, upperInclusive));
      return this;
    } catch (BsonParseException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public RangeOperatorBuilder doesNotAffect(List<String> doesNotAffect) {
    this.doesNotAffect = Optional.of(doesNotAffect);
    return this;
  }

  public RangeOperatorBuilder doesNotAffect(String doesNotAffect) {
    this.doesNotAffect = Optional.of(List.of(doesNotAffect));
    return this;
  }

  @Override
  public RangeOperator build() {
    Check.isPresent(this.range, "range");

    return new RangeOperator(getScore(), getPaths(), this.range.get(), this.doesNotAffect);
  }
}
