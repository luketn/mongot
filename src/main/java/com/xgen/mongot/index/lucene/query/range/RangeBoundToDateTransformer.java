package com.xgen.mongot.index.lucene.query.range;

import com.xgen.mongot.index.query.operators.bound.RangeBound;
import com.xgen.mongot.index.query.points.DatePoint;
import java.util.Date;

/**
 * Take a RangeBoundDefinition and transform it into a Date. Depends on if this is a lower or an
 * upper bound and if this bound is inclusive or exclusive.
 */
public class RangeBoundToDateTransformer {

  private static final Date MIN_VALUE = new Date(Long.MIN_VALUE);
  private static final Date MAX_VALUE = new Date(Long.MAX_VALUE);

  public static Date getLower(RangeBound<DatePoint> bound) {
    if (bound.getLower().isEmpty()) {
      // If the lower bound is undefined, the lower bound is the minimum representable value.
      return MIN_VALUE;
    }
    return getValue(bound.getLower().get(), bound.lowerInclusive(), true);
  }

  public static Date getUpper(RangeBound<DatePoint> bound) {
    if (bound.getUpper().isEmpty()) {
      // If the upper bound is undefined, the upper bound is the maximum representable value.
      return MAX_VALUE;
    }
    return getValue(bound.getUpper().get(), bound.upperInclusive(), false);
  }

  private static Date getValue(DatePoint datePoint, boolean inclusive, boolean lower) {
    return inclusive
        ? datePoint.value()
        : lower ? increment(datePoint.value()) : decrement(datePoint.value());
  }

  private static Date increment(Date value) {
    return new Date(Math.addExact(Math.min(Long.MAX_VALUE - 1, value.getTime()), 1));
  }

  private static Date decrement(Date value) {
    return new Date(Math.subtractExact(Math.max(Long.MIN_VALUE + 1, value.getTime()), 1));
  }
}
