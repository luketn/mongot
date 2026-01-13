package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;

public abstract class Result implements DocumentEncodable {
  public enum Type {
    VALID,
    INVALID,
  }

  static class Fields {
    static final Field.Required<Boolean> VALID = Field.builder("valid").booleanField().required();
  }

  /** Package-private constructor to ensure only classes in this package can implement Result. */
  Result() {}

  static Result fromBson(DocumentParser parser) throws BsonParseException {
    boolean valid = parser.getField(Fields.VALID).unwrap();
    return valid ? ValidResult.fromBson(parser) : InvalidResult.fromBson(parser);
  }

  public abstract Type getType();

  public ValidResult asValid() {
    throwIfInvalidType(Type.VALID);
    return (ValidResult) this;
  }

  public InvalidResult asInvalid() {
    throwIfInvalidType(Type.INVALID);
    return (InvalidResult) this;
  }

  public boolean isValid() {
    return this instanceof ValidResult;
  }

  public boolean isInvalid() {
    return this instanceof InvalidResult;
  }

  private void throwIfInvalidType(Type expectedType) {
    Check.expectedType(expectedType, getType());
  }
}
