package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

public record StringFacetFieldDefinition()
    implements FieldTypeDefinition, FacetableStringFieldDefinition {
  static StringFacetFieldDefinition fromBson(DocumentParser parser) {
    return new StringFacetFieldDefinition();
  }

  @Override
  public Type getType() {
    return Type.STRING_FACET;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }
}
