package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

public record ObjectIdFieldDefinition() implements FieldTypeDefinition {
  static ObjectIdFieldDefinition fromBson(DocumentParser parser) {
    return new ObjectIdFieldDefinition();
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }

  @Override
  public Type getType() {
    return Type.OBJECT_ID;
  }
}
