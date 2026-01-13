package com.xgen.proto;

import org.bson.BsonReader;
import org.bson.BsonType;

public class NumericUtils {
  private NumericUtils() {}

  private static final String INT32_TYPE = "32-bit integer";
  private static final String INT64_TYPE = "64-bit integer";
  private static final String FLOAT_TYPE = "single precision float";

  private static final float MAX_FLOAT = Float.MAX_VALUE;
  private static final float MIN_FLOAT = -1 * Float.MAX_VALUE;

  private static BsonProtoParseException handleOverflow(String description, String fieldName)
      throws BsonProtoParseException {
    throw new BsonProtoParseException(
        String.format("Value for %s overflowed, must fit in %s", fieldName, description));
  }

  private static BsonProtoParseException handleUnderflow(String description, String fieldName)
      throws BsonProtoParseException {
    throw new BsonProtoParseException(
        String.format("Value for %s underflowed, must fit in %s", fieldName, description));
  }

  /**
   * Coerce a Bson value in a stream into a 32-bit integer.
   *
   * @param reader where the type and name of the current value have already been read.
   * @return the current value as an int.
   * @throws BsonProtoParseException if the type cannot be coerced to an int or if coercion would
   *     cause the value to overflow or underflow.
   */
  public static int readIntValue(String fieldName, BsonReader reader)
      throws BsonProtoParseException {
    switch (reader.getCurrentBsonType()) {
      case INT32:
        return reader.readInt32();
      case INT64:
        long longValue = reader.readInt64();
        if (longValue > Integer.MAX_VALUE) {
          handleOverflow(INT32_TYPE, fieldName);
        } else if (longValue < Integer.MIN_VALUE) {
          handleUnderflow(INT32_TYPE, fieldName);
        }
        return (int) longValue;
      case DOUBLE:
        double doubleValue = reader.readDouble();
        if (doubleValue > Integer.MAX_VALUE) {
          handleOverflow(INT32_TYPE, fieldName);
        } else if (doubleValue < Integer.MIN_VALUE) {
          handleUnderflow(INT32_TYPE, fieldName);
        }

        // We cannot use any double with a fractional value.
        if (doubleValue % 1 != 0) {
          throw new BsonProtoParseException(
              String.format(
                  "Cannot use fractional double value for field %s of type %s",
                  fieldName, INT32_TYPE));
        }

        return (int) doubleValue;
      default:
        throw BsonProtoParseException.createTypeMismatchException(
            BsonType.INT32, reader.getCurrentBsonType(), fieldName);
    }
  }

  /**
   * Coerce a Bson value in a stream into a 64-bit integer.
   *
   * @param reader where the type and name of the current value have already been read.
   * @return the current value as a long.
   * @throws BsonProtoParseException if the type cannot be coerced to a long or if coercion would
   *     cause the value to overflow or underflow.
   */
  public static long readLongValue(String fieldName, BsonReader reader)
      throws BsonProtoParseException {
    switch (reader.getCurrentBsonType()) {
      case INT32:
        return reader.readInt32();
      case INT64:
        return reader.readInt64();
      case DOUBLE:
        double doubleValue = reader.readDouble();
        if (doubleValue > Long.MAX_VALUE) {
          handleOverflow(INT64_TYPE, fieldName);
        } else if (doubleValue < Long.MIN_VALUE) {
          handleUnderflow(INT64_TYPE, fieldName);
        }

        // We cannot use any double with a fractional value.
        if (doubleValue % 1 != 0) {
          throw new BsonProtoParseException(
              String.format(
                  "Cannot use fractional double value for field %s of type %s",
                  fieldName, INT64_TYPE));
        }

        return (int) doubleValue;
      default:
        throw BsonProtoParseException.createTypeMismatchException(
            BsonType.INT64, reader.getCurrentBsonType(), fieldName);
    }
  }

  /**
   * Coerce a Bson value in a stream into a single precision float value.
   *
   * @param reader where the type and name of the current value have already been read.
   * @return the current value as a float.
   * @throws BsonProtoParseException if the type cannot be coerced to a float or if coercion would
   *     cause the value to overflow or underflow.
   */
  public static float readFloatValue(String fieldName, BsonReader reader)
      throws BsonProtoParseException {
    switch (reader.getCurrentBsonType()) {
      case INT32:
        return (float) reader.readInt32();
      case INT64:
        return (float) reader.readInt64();
      case DOUBLE:
        double doubleValue = reader.readDouble();

        // Explicitly map special values before checking for over-/underflow.
        if (doubleValue == Double.NEGATIVE_INFINITY) {
          return Float.NEGATIVE_INFINITY;
        } else if (doubleValue == Double.POSITIVE_INFINITY) {
          return Float.POSITIVE_INFINITY;
        } else if (Double.isNaN(doubleValue)) {
          return Float.NaN;
        }

        if (doubleValue > MAX_FLOAT) {
          handleOverflow(FLOAT_TYPE, fieldName);
        } else if (doubleValue < MIN_FLOAT) {
          handleUnderflow(FLOAT_TYPE, fieldName);
        }

        return (float) doubleValue;
      default:
        throw BsonProtoParseException.createTypeMismatchException(
            BsonType.DOUBLE, reader.getCurrentBsonType(), fieldName);
    }
  }

  /**
   * Coerce a Bson value in a stream into a double precision float value.
   *
   * @param reader where the type and name of the current value have already been read.
   * @return the current value as a double.
   * @throws BsonProtoParseException if the type cannot be coerced to a double or if coercion would
   *     cause the value to overflow or underflow.
   */
  public static double readDoubleValue(String fieldName, BsonReader reader)
      throws BsonProtoParseException {
    return switch (reader.getCurrentBsonType()) {
      case INT32 -> reader.readInt32();
      case INT64 -> reader.readInt64();
      case DOUBLE -> reader.readDouble();
      default ->
          throw BsonProtoParseException.createTypeMismatchException(
              BsonType.DOUBLE, reader.getCurrentBsonType(), fieldName);
    };
  }
}
