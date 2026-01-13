package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

public record SortableDateBetaV1FieldDefinition() implements FieldTypeDefinition {
  static SortableDateBetaV1FieldDefinition fromBson(DocumentParser parser) {
    return new SortableDateBetaV1FieldDefinition();
  }

  @Override
  public Type getType() {
    return Type.SORTABLE_DATE_BETA_V1;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }
}
