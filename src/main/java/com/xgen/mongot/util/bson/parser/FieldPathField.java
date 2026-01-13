package com.xgen.mongot.util.bson.parser;

import com.xgen.mongot.util.FieldPath;
import org.bson.BsonString;
import org.bson.BsonType;
import org.bson.BsonValue;

public class FieldPathField {
  /** Deserialize a FieldPath. */
  public static FieldPath parse(BsonParseContext context, BsonValue value)
      throws BsonParseException {
    if (value.getBsonType() != BsonType.STRING) {
      context.handleUnexpectedType(TypeDescription.STRING, value.getBsonType());
    }

    return FieldPath.parse(value.asString().getValue());
  }

  public static BsonValue encode(FieldPath value) {
    return new BsonString(value.toString());
  }
}
