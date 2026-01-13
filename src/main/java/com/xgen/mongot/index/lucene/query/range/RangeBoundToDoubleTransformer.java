package com.xgen.mongot.index.lucene.query.range;

import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;

/**
 * Take a RangeBoundDefinition and transform it into a Double. Depends on if this is a lower or an
 * upper bound, if this bound is inclusive or exclusive, and if the bound was specified as a long or
 * as a double.
 */
public class RangeBoundToDoubleTransformer {

  private static final double MIN_VALUE = -1.0 * Double.MAX_VALUE;
  private static final double MAX_VALUE = Double.MAX_VALUE;

  public static double getLower(RangeBound<NumericPoint> bound) {
    if (bound.getLower().isEmpty()) {
      // If the lower bound is undefined, the lower bound is the minimum representable value.
      return MIN_VALUE;
    }
    return getValue(bound.getLower().get(), bound.lowerInclusive(), true);
  }

  public static double getUpper(RangeBound<NumericPoint> bound) {
    if (bound.getUpper().isEmpty()) {
      // If the upper bound is undefined, the upper bound is the maximum representable value.
      return MAX_VALUE;
    }
    return getValue(bound.getUpper().get(), bound.upperInclusive(), false);
  }

  private static double getValue(NumericPoint point, boolean inclusive, boolean lower) {
    return switch (point) {
      case LongPoint lp -> DoubleFromLongSupplier.getValue(lp, inclusive, lower);
      case DoublePoint dp -> DoubleFromDoubleSupplier.getValue(dp, inclusive, lower);
    };
  }

  /** User specified bound as a Long. */
  private static class DoubleFromLongSupplier {
    static double lowerInclusive(LongPoint point) {
      return (double) point.value();
    }

    static double lowerExclusive(LongPoint point) {
      return Math.nextUp((double) point.value());
    }

    static double upperInclusive(LongPoint point) {
      return (double) point.value();
    }

    static double upperExclusive(LongPoint point) {
      return Math.nextDown((double) point.value());
    }

    static double getValue(LongPoint point, boolean inclusive, boolean lower) {
      return inclusive
          ? (lower ? lowerInclusive(point) : upperInclusive(point))
          : (lower ? lowerExclusive(point) : upperExclusive(point));
    }
  }

  /** User specified bound as a Double. */
  private static class DoubleFromDoubleSupplier {
    static double lowerInclusive(DoublePoint point) {
      return point.value();
    }

    static double lowerExclusive(DoublePoint point) {
      return Math.nextUp(point.value());
    }

    static double upperInclusive(DoublePoint point) {
      return point.value();
    }

    static double upperExclusive(DoublePoint point) {
      return Math.nextDown(point.value());
    }

    static double getValue(DoublePoint point, boolean inclusive, boolean lower) {
      return inclusive
          ? (lower ? lowerInclusive(point) : upperInclusive(point))
          : (lower ? lowerExclusive(point) : upperExclusive(point));
    }
  }
}
