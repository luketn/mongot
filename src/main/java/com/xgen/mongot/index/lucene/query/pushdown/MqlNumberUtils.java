package com.xgen.mongot.index.lucene.query.pushdown;

import org.bson.BsonDouble;
import org.bson.BsonNumber;
import org.bson.types.Decimal128;

class MqlNumberUtils {

  static int compare(double left, BsonNumber right) {
    return switch (right.getBsonType()) { // Promotion from int -> double always safe
      case INT32, DOUBLE -> MqlDouble.compare(left, right.doubleValue());
      case INT64 -> MqlDouble.compare(left, right.longValue());
      case DECIMAL128 ->
          // new Decimal128(new BidDecimal(double)) fails on NaN and infinities
          compare(new BsonDouble(left).decimal128Value(), right.decimal128Value());
      default -> throw new AssertionError();
    };
  }

  static int compare(Decimal128 left, Decimal128 right) {
    if (left.isNaN() || right.isNaN()) {
      // MQL considers NaN < MIN_VALUE, which is the opposite of the Java driver
      return Boolean.compare(right.isNaN(), left.isNaN());
    } else if (left.isFinite() && right.isFinite()) {
      boolean leftNegative = left.isNegative();
      boolean rightNegative = right.isNegative();
      if (leftNegative != rightNegative) {
        if (leftNegative && left.compareTo(Decimal128.NEGATIVE_ZERO) == 0) {
          return right.compareTo(Decimal128.POSITIVE_ZERO) == 0 ? 0 : -1;
        } else if (rightNegative && right.compareTo(Decimal128.NEGATIVE_ZERO) == 0) {
          return left.compareTo(Decimal128.POSITIVE_ZERO) == 0 ? 0 : 1;
        } else {
          // Values are not NaN, have different signs, at least one is non-zero
          return Boolean.compare(rightNegative, leftNegative);
        }
      }
      return left.compareTo(right);
    } else {
      return left.compareTo(right);
    }
  }

  static int compare(long left, BsonNumber right) {
    return switch (right.getBsonType()) {
      case INT32, INT64 -> Long.compare(left, right.longValue());
      case DOUBLE -> MqlDouble.compare(left, right.doubleValue());
      case DECIMAL128 -> compare(new Decimal128(left), right.decimal128Value());
      default -> throw new AssertionError();
    };
  }

  static int compare(int left, BsonNumber right) {
    return switch (right.getBsonType()) {
      case INT32 -> Integer.compare(left, right.intValue());
      case INT64 -> Long.compare(left, right.longValue());
      case DOUBLE ->
          // int is always safe to promote to double
          MqlDouble.compare((double) left, right.doubleValue());
      case DECIMAL128 -> compare(new Decimal128(left), right.decimal128Value());
      default -> throw new AssertionError();
    };
  }
}
