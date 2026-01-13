package com.xgen.proto;

import java.util.Collection;
import java.util.stream.Collectors;
import org.bson.BsonType;

public class BsonProtoParseException extends Exception {
  public BsonProtoParseException(String message) {
    super(message);
  }

  public static BsonProtoParseException createTypeMismatchException(
      BsonType expectedType, BsonType actualType, String fieldName) {
    return new BsonProtoParseException(
        String.format(
            "Expected type %s but got type %s for %s.", expectedType, actualType, fieldName));
  }

  public static BsonProtoParseException createTypeMismatchException(
      Collection<BsonType> expectedTypes, BsonType actualType, String message) {
    return new BsonProtoParseException(
        String.format(
            "Expected type to be one of %s got %s: %s",
            expectedTypes.stream().map(BsonType::toString).collect(Collectors.joining(",")),
            actualType,
            message));
  }

  public static void throwTypeMismatchException(
      BsonType expectedType, BsonType actualType, String message) throws BsonProtoParseException {
    throw createTypeMismatchException(expectedType, actualType, message);
  }

  public static void throwTypeMismatchException(
      Collection<BsonType> expectedTypes, BsonType actualType, String message)
      throws BsonProtoParseException {
    throw createTypeMismatchException(expectedTypes, actualType, message);
  }
}
