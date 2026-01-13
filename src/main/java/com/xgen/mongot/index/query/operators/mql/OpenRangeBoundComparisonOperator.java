package com.xgen.mongot.index.query.operators.mql;

import static com.xgen.mongot.index.query.points.Point.Type.BOOLEAN;
import static com.xgen.mongot.index.query.points.Point.Type.DATE;
import static com.xgen.mongot.index.query.points.Point.Type.NUMBER;
import static com.xgen.mongot.index.query.points.Point.Type.OBJECT_ID;
import static com.xgen.mongot.index.query.points.Point.Type.STRING;
import static com.xgen.mongot.index.query.points.Point.Type.UUID;
import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.base.Objects;
import com.xgen.mongot.index.query.operators.bound.BooleanRangeBound;
import com.xgen.mongot.index.query.operators.bound.DateRangeBound;
import com.xgen.mongot.index.query.operators.bound.NumericRangeBound;
import com.xgen.mongot.index.query.operators.bound.ObjectIdRangeBound;
import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.operators.bound.StringRangeBound;
import com.xgen.mongot.index.query.operators.bound.UuidRangeBound;
import com.xgen.mongot.index.query.operators.type.ValueType;
import com.xgen.mongot.index.query.points.BooleanPoint;
import com.xgen.mongot.index.query.points.DatePoint;
import com.xgen.mongot.index.query.points.GeoPoint;
import com.xgen.mongot.index.query.points.NumericPoint;
import com.xgen.mongot.index.query.points.ObjectIdPoint;
import com.xgen.mongot.index.query.points.Point;
import com.xgen.mongot.index.query.points.StringPoint;
import com.xgen.mongot.index.query.points.UuidPoint;
import com.xgen.mongot.util.bson.parser.BsonParseContext;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import java.util.EnumSet;
import java.util.Optional;

public abstract sealed class OpenRangeBoundComparisonOperator implements ComparisonOperator
    permits GtOperator, GteOperator, LtOperator, LteOperator {

  public static final EnumSet<Point.Type> SUPPORTED_POINT_TYPES =
      EnumSet.of(BOOLEAN, DATE, NUMBER, OBJECT_ID, STRING, UUID);

  private final RangeBound<? extends Comparable<?>> bounds;

  static class Values {
    static final com.xgen.mongot.util.bson.parser.Value.Required<Point> VALUE =
        com.xgen.mongot.util.bson.parser.Value.builder()
            .classValue(Point::fromBson)
            .validate(
                point ->
                    SUPPORTED_POINT_TYPES.contains(point.getType())
                        ? Optional.empty()
                        : Optional.of("Type is not supported: " + point.getType()))
            .required();
  }

  protected OpenRangeBoundComparisonOperator(RangeBound<? extends Comparable<?>> bounds) {
    this.bounds = bounds;
  }

  public RangeBound<? extends Comparable<?>> getBounds() {
    return this.bounds;
  }

  @Override
  public ValueType getValueType() {
    return this.bounds.getType();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OpenRangeBoundComparisonOperator that = (OpenRangeBoundComparisonOperator) o;
    return Objects.equal(this.bounds, that.bounds);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.bounds);
  }

  static RangeBound<? extends Comparable<?>> convertToRangeBounds(
      BsonParseContext context,
      Optional<Point> lowerBound,
      Optional<Point> upperBound,
      boolean lowerInclusive,
      boolean upperInclusive)
      throws BsonParseException {

    checkArg(
        lowerBound.isPresent() ^ upperBound.isPresent(),
        "either lowerBound or upperBound should be present, got %s and %s",
        lowerBound,
        upperBound);

    var bound = lowerBound.orElseGet(upperBound::get);
    checkArg(
        SUPPORTED_POINT_TYPES.contains(bound.getType()),
        "%s type is not supported",
        bound.getType());

    return switch (bound) {
      case BooleanPoint booleanPoint ->
          BooleanRangeBound.create(
              context,
              lowerBound.map((p) -> booleanPoint),
              upperBound.map((p) -> booleanPoint),
              lowerInclusive,
              upperInclusive);
      case DatePoint datePoint ->
          DateRangeBound.create(
              context,
              lowerBound.map((p) -> datePoint),
              upperBound.map((p) -> datePoint),
              lowerInclusive,
              upperInclusive);
      case NumericPoint numericPoint ->
          NumericRangeBound.create(
              context,
              lowerBound.map((p) -> numericPoint),
              upperBound.map((p) -> numericPoint),
              lowerInclusive,
              upperInclusive);
      case ObjectIdPoint objectIdPoint ->
          ObjectIdRangeBound.create(
              context,
              lowerBound.map((p) -> objectIdPoint),
              upperBound.map((p) -> objectIdPoint),
              lowerInclusive,
              upperInclusive);
      case StringPoint stringPoint ->
          StringRangeBound.create(
              context,
              lowerBound.map((p) -> stringPoint),
              upperBound.map((p) -> stringPoint),
              lowerInclusive,
              upperInclusive);
      case UuidPoint uuidPoint ->
          UuidRangeBound.create(
              context,
              lowerBound.map((p) -> uuidPoint),
              upperBound.map((p) -> uuidPoint),
              lowerInclusive,
              upperInclusive);
      case GeoPoint geoPoint ->
          throw new IllegalArgumentException(
              String.format("%s type is not supported", geoPoint.getType()));
    };
  }
}
