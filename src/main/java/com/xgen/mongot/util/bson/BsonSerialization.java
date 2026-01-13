package com.xgen.mongot.util.bson;

import java.util.Collection;

// Helpers intended to be useful in BSON (de)serialization.
public class BsonSerialization {

  /** Throws a BsonMissingPropertyException if the supplied Object is null. */
  public static void throwIfMissingProperty(Object obj, String propertyName)
      throws BsonMissingPropertyException {
    if (obj == null) {
      throw new BsonMissingPropertyException(propertyName);
    }
  }

  /** Throws a BsonMissingPropertyException if the supplied Collection is null or empty. */
  public static void throwIfMissingOrEmptyProperty(Collection<?> obj, String propertyName)
      throws BsonMissingPropertyException {
    throwIfMissingProperty(obj, propertyName);
    if (obj.isEmpty()) {
      throw new BsonMissingPropertyException(propertyName);
    }
  }

  /** Throws a BsonMissingPropertyException if the supplied String is null or empty. */
  public static void throwIfMissingOrEmptyProperty(String str, String propertyName)
      throws BsonMissingPropertyException {
    throwIfMissingProperty(str, propertyName);
    if (str.isEmpty()) {
      throw new BsonMissingPropertyException(propertyName);
    }
  }

  /**
   * Throws a BsonMissingPropertyException if the supplied Collection is null, empty, or has any
   * elements that are empty strings.
   */
  public static void throwIfAnyEmptyElements(Collection<String> obj, String propertyName)
      throws BsonMissingPropertyException {
    throwIfMissingOrEmptyProperty(obj, propertyName);
    obj.forEach(str -> throwIfMissingOrEmptyProperty(str, propertyName));
  }
}
