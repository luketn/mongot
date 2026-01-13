package com.xgen.mongot.util;

/**
 * This class defines double/long ordering for NUMBER_V1 fields in equals and range queries.
 *
 * <p>Notably, this differs from NUMBER_V2 which treats NaN < NEGATIVE_INFINITY and both of these
 * differ from MQL which treats NAN < NEGATIVE_INFINITY and 0.0 == -0.0.
 */
public class NumericUtils {

  public static int compareDoubleLong(double a, long b) {
    // TODO(CLOUDP-280897): This method is inaccurate for high magnitude long values
    //  see https://github.com/mongodb/mongo/blob/master/src/mongo/base/compare_numbers.h#L77-L100
    return a < b ? -1 : a > b ? 1 : 0;
  }

  public static int compareLongDouble(long a, double b) {
    // TODO(CLOUDP-280897): This method is inaccurate for high magnitude long values:
    //  see https://github.com/mongodb/mongo/blob/master/src/mongo/base/compare_numbers.h#L77-L100
    return a < b ? -1 : a > b ? 1 : 0;
  }

  public static int compareLongLong(long a, long b) {
    return Long.compare(a, b);
  }

  public static int compareDoubleDouble(double a, double b) {
    return Double.compare(a, b);
  }
}
