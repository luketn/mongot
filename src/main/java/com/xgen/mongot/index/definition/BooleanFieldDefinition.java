package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

/**
 * BooleanFieldDefinition defines how to index fields of type "boolean".
 *
 * <p>This class is empty; there are no configuration options for a boolean field definition.
 */
public record BooleanFieldDefinition() implements FieldTypeDefinition {
  static BooleanFieldDefinition fromBson(DocumentParser parser) {
    return new BooleanFieldDefinition();
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }

  @Override
  public Type getType() {
    return Type.BOOLEAN;
  }
}
