package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

public record UuidFieldDefinition() implements FieldTypeDefinition {
  static UuidFieldDefinition fromBson(DocumentParser parser) {
    return new UuidFieldDefinition();
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }

  @Override
  public Type getType() {
    return Type.UUID;
  }
}
