package com.xgen.mongot.util.bson;

import static com.xgen.mongot.util.NumericUtils.compareDoubleDouble;
import static com.xgen.mongot.util.NumericUtils.compareDoubleLong;
import static com.xgen.mongot.util.NumericUtils.compareLongDouble;
import static com.xgen.mongot.util.NumericUtils.compareLongLong;

import com.xgen.mongot.util.Check;
import org.bson.BsonNumber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BsonNumberUtils {
  private static final Logger logger = LoggerFactory.getLogger(BsonNumberUtils.class);

  /** Compares two BsonNumbers. */
  public static int compare(BsonNumber n1, BsonNumber n2) {
    return switch (n1.getBsonType()) {
      case INT32 -> compareLong(n1.asInt32().longValue(), n2);
      case INT64 -> compareLong(n1.asInt64().getValue(), n2);
      case DOUBLE -> compareDouble(n1.asDouble().getValue(), n2);
      case DECIMAL128 -> throw new IllegalArgumentException("Cannot compare decimal128.");
      default -> {
        logger.error("Unexpected BsonType: {}", n1.getBsonType());
        yield Check.unreachable("Unexpected BsonType");
      }
    };
  }

  private static int compareLong(long n1, BsonNumber n2) {
    return switch (n2.getBsonType()) {
      case INT32 -> compareLongLong(n1, n2.asInt32().longValue());
      case INT64 -> compareLongLong(n1, n2.asInt64().getValue());
      case DOUBLE -> compareLongDouble(n1, n2.asDouble().getValue());
      case DECIMAL128 -> throw new IllegalArgumentException("Cannot compare decimal128.");
      default -> {
        logger.error("Unexpected BsonType: {}", n2.getBsonType());
        yield Check.unreachable("Unexpected BsonType");
      }
    };
  }

  private static int compareDouble(double n1, BsonNumber n2) {
    return switch (n2.getBsonType()) {
      case INT32 -> compareDoubleLong(n1, n2.asInt32().longValue());
      case INT64 -> compareDoubleLong(n1, n2.asInt64().getValue());
      case DOUBLE -> compareDoubleDouble(n1, n2.asDouble().getValue());
      case DECIMAL128 -> throw new IllegalArgumentException("Cannot compare decimal128.");
      default -> {
        logger.error("Unexpected BsonType: {}", n2.getBsonType());
        yield Check.unreachable("Unexpected BsonType");
      }
    };
  }
}
