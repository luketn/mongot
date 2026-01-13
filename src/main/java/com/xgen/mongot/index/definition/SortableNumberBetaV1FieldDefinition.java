package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Objects;
import org.bson.BsonDocument;

public record SortableNumberBetaV1FieldDefinition() implements FieldTypeDefinition {
  static SortableNumberBetaV1FieldDefinition fromBson(DocumentParser parser) {
    return new SortableNumberBetaV1FieldDefinition();
  }

  @Override
  public Type getType() {
    return Type.SORTABLE_NUMBER_BETA_V1;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return new BsonDocument();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SortableNumberBetaV1FieldDefinition;
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }
}
