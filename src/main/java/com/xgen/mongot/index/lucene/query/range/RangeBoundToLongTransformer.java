package com.xgen.mongot.index.lucene.query.range;

import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.DoublePoint;
import com.xgen.mongot.index.query.points.LongPoint;
import com.xgen.mongot.index.query.points.NumericPoint;

/**
 * Take a RangeBoundDefinition and transform it into a Long. Depends on if this is a lower or an
 * upper bound, if this bound is inclusive or exclusive, and if the bound was specified as a long or
 * as a double.
 */
public class RangeBoundToLongTransformer {

  private static final long MIN_VALUE = Long.MIN_VALUE;
  private static final long MAX_VALUE = Long.MAX_VALUE;

  public static long getLower(RangeBound<NumericPoint> bound) {
    if (bound.getLower().isEmpty()) {
      // If the lower bound is undefined, the lower bound is the minimum representable value.
      return MIN_VALUE;
    }
    return getValue(bound.getLower().get(), bound.lowerInclusive(), true);
  }

  public static long getUpper(RangeBound<NumericPoint> bound) {
    if (bound.getUpper().isEmpty()) {
      // If the upper bound is undefined, the upper bound is the maximum representable value.
      return MAX_VALUE;
    }
    return getValue(bound.getUpper().get(), bound.upperInclusive(), false);
  }

  private static long getValue(NumericPoint point, boolean inclusive, boolean lower) {
    return switch (point) {
      case LongPoint lp -> LongFromLongSupplier.getValue(lp, inclusive, lower);
      case DoublePoint dp -> LongFromDoubleSupplier.getValue(dp, inclusive, lower);
    };
  }

  /** User specified bound as a Long. */
  private static class LongFromLongSupplier {
    static long lowerInclusive(LongPoint point) {
      return point.value();
    }

    static long lowerExclusive(LongPoint point) {
      return Math.addExact(point.value(), 1);
    }

    static long upperInclusive(LongPoint point) {
      return point.value();
    }

    static long upperExclusive(LongPoint point) {
      return Math.subtractExact(point.value(), 1);
    }

    static long getValue(LongPoint point, boolean inclusive, boolean lower) {
      return inclusive
          ? (lower ? lowerInclusive(point) : upperInclusive(point))
          : (lower ? lowerExclusive(point) : upperExclusive(point));
    }
  }

  /** User specified bound as a Double. */
  private static class LongFromDoubleSupplier {
    static Long lowerInclusive(DoublePoint point) {
      return Double.valueOf(Math.ceil(point.value())).longValue();
    }

    static Long lowerExclusive(DoublePoint point) {
      return Double.valueOf(Math.max(Math.ceil(Math.nextUp(point.value())), Long.MIN_VALUE))
          .longValue();
    }

    static Long upperInclusive(DoublePoint point) {
      return Double.valueOf(Math.floor(point.value())).longValue();
    }

    static Long upperExclusive(DoublePoint point) {
      return Double.valueOf(Math.min(Math.floor(Math.nextDown(point.value())), Long.MAX_VALUE))
          .longValue();
    }

    static Long getValue(DoublePoint point, boolean inclusive, boolean lower) {
      return inclusive
          ? (lower ? lowerInclusive(point) : upperInclusive(point))
          : (lower ? lowerExclusive(point) : upperExclusive(point));
    }
  }
}
