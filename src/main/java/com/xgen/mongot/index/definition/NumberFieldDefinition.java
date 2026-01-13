package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

/** A NumberFieldDefinition defines how mongot should index numbers for a single field. */
public record NumberFieldDefinition(NumericFieldOptions options)
    implements FieldTypeDefinition, NumericFieldDefinition {
  static NumberFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new NumberFieldDefinition(NumericFieldOptions.fromBson(parser));
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return this.options.toBson();
  }

  @Override
  public boolean hasSameOptionsAs(NumericFieldDefinition other) {
    return this.options.equals(other.options());
  }

  @Override
  public Type getType() {
    return Type.NUMBER;
  }
}
