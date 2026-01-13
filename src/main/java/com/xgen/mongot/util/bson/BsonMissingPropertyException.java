package com.xgen.mongot.util.bson;

import com.xgen.mongot.util.LoggableRuntimeException;

/**
 * BsonMissingPropertyException should be thrown when a required property is missing during BSON
 * serialization or deserialization.
 */
public class BsonMissingPropertyException extends LoggableRuntimeException {

  public BsonMissingPropertyException(String propertyName) {
    super(String.format("property %s is required", propertyName));
  }
}
