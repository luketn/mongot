package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

public record DateFacetFieldDefinition() implements FieldTypeDefinition, DatetimeFieldDefinition {
  static DateFacetFieldDefinition fromBson(DocumentParser parser) {
    return new DateFacetFieldDefinition();
  }

  @Override
  public Type getType() {
    return Type.DATE_FACET;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }
}
