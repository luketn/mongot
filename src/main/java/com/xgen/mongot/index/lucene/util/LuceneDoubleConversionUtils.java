package com.xgen.mongot.index.lucene.util;

import org.apache.lucene.util.NumericUtils;

public class LuceneDoubleConversionUtils {

  /**
   * After encoding, there is plenty of room between toMqlSortableLong(-Infinity)
   * (-921886843722740531) and Long.MIN_VALUE (-9223372036854775808). We want to make NaN encoded to
   * be less than all other possible double values, but we also want to leave one other sentinel
   * value below it in case we want to be able to encode the concept of a value smaller than NaN
   * (e.g. "missing"), so we encode it as Long.MIN_VALUE + 1.
   */
  public static final long NAN_SENTINEL = Long.MIN_VALUE + 1;

  /**
   * Transforms a double value into a long one. Uses a bitmapping described in detail in the <a
   * href="https://github.com/10gen/mongot/blob/master/docs/internal/numeric-index-representation.md">
   * documentation</a>.
   *
   * <p>Use the {@code fromLong} function in this class to change transformed values back to their
   * original double format.
   *
   * @param value floating point number to transform
   * @return mapped long bits
   */
  public static long toLong(double value) {
    return (Double.compare(value, 0) >= 0)
        ? Double.doubleToLongBits(value)
        : (0x7FFFFFFFFFFFFFFFL ^ Double.doubleToLongBits(value)) + 1;
  }

  public static long toIndexedLong(long value) {
    return toLong((double) value);
  }

  /**
   * Converts a double into a long in a way that preserves sort order and considers NaN <
   * NEGATIVE_INFINITY. Distinct NaN bit patterns are not preserved.
   */
  public static long toMqlSortableLong(double value) {
    if (Double.isNaN(value)) {
      return NAN_SENTINEL;
    }

    // See the comment in MqlDoubleComparator::getValueForDoc on why we use Lucene's
    // doubleToSortableLong() instead of our toLong().
    return NumericUtils.doubleToSortableLong(value);
  }

  public static long toMqlIndexedLong(long value) {
    return toMqlSortableLong((double) value);
  }

  /** The inverse of {@link #toMqlSortableLong(double)} */
  public static double fromMqlSortableLong(long value) {
    if (value == NAN_SENTINEL) {
      return Double.NaN;
    }
    return NumericUtils.sortableLongToDouble(value);
  }

  /**
   * Transforms a value transformed with {@code toLong} back to it's original double format. Uses a
   * bitmapping described in detail in the <a
   * href="https://github.com/10gen/mongot/blob/master/docs/internal/numeric-index-representation.md">
   * documentation</a>.
   *
   * @param value mapped long bits
   * @return untransformed floating point number
   */
  public static double fromLong(long value) {
    return value >= 0
        ? Double.longBitsToDouble(value)
        : Double.longBitsToDouble((0x7FFFFFFFFFFFFFFFL ^ value) + 1);
  }
}
