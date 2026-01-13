package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

public record SortableStringBetaV1FieldDefinition() implements FieldTypeDefinition {
  static SortableStringBetaV1FieldDefinition fromBson(DocumentParser parser) {
    return new SortableStringBetaV1FieldDefinition();
  }

  @Override
  public Type getType() {
    return Type.SORTABLE_STRING_BETA_V1;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }
}
