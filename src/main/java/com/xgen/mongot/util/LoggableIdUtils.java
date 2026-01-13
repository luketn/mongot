package com.xgen.mongot.util;

import java.util.Optional;
import java.util.regex.Pattern;
import org.bson.BsonBinarySubType;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

/**
 * Utility class for creating loggable document IDs that avoid PII leakage.
 *
 * <p>UUIDs and ObjectIds are returned as-is without hashing. Other ID types cannot be logged and
 * return "unloggable".
 */
public final class LoggableIdUtils {

  /** The string returned when a document ID cannot be logged to avoid PII leakage. */
  public static final String UNKNOWN_LOGGABLE_ID = "unknown";

  /** The string returned when an ID type cannot be logged. */
  public static final String UNLOGGABLE_ID_TYPE = "unloggable";

  /**
   * Pattern to match UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
   *
   * <p>Matches RFC 4122 standard UUID format (36 characters including hyphens), compatible with
   * MongoDB UUID() method and Java UUID.fromString().
   */
  private static final Pattern UUID_PATTERN =
      Pattern.compile(
          "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

  private LoggableIdUtils() {
    // Utility class, prevent instantiation
  }

  /**
   * Returns a loggable ID string from an optional document ID.
   *
   * <p>If the ID is a UUID or ObjectId, it is returned as-is without hashing. Other ID types cannot
   * be logged and return "unloggable".
   *
   * @param id optional document ID string
   * @return a loggable ID string - original value if it's a UUID or ObjectId, otherwise
   *     "unloggable"
   */
  public static String getLoggableId(Optional<String> id) {
    if (id.isEmpty()) {
      return UNKNOWN_LOGGABLE_ID;
    }

    String idString = id.get();

    // Check if it's a valid ObjectId - return as-is
    if (ObjectId.isValid(idString)) {
      return idString;
    }

    // Check if it's a valid UUID - return as-is
    if (UUID_PATTERN.matcher(idString).matches()) {
      return idString;
    }

    // Other types cannot be logged
    return UNLOGGABLE_ID_TYPE;
  }

  /**
   * Returns a loggable ID string from a BsonValue document ID.
   *
   * <p>If the ID is a UUID or ObjectId, it is returned as-is without hashing. Other ID types cannot
   * be logged and return "unloggable".
   *
   * @param id BsonValue document ID, may be null
   * @return a loggable ID string - original value if it's a UUID or ObjectId, otherwise
   *     "unloggable"
   */
  public static String getLoggableId(BsonValue id) {
    if (id == null) {
      return UNKNOWN_LOGGABLE_ID;
    }

    BsonType bsonType = id.getBsonType();

    // Return ObjectId as-is
    if (bsonType == BsonType.OBJECT_ID) {
      return id.asObjectId().getValue().toHexString();
    }

    // Return UUID as-is
    if (bsonType == BsonType.BINARY && BsonBinarySubType.isUuid(id.asBinary().getType())) {
      return id.asBinary().asUuid().toString();
    }

    // Other types cannot be logged
    return UNLOGGABLE_ID_TYPE;
  }
}
