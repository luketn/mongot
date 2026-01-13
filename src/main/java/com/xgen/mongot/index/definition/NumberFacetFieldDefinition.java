package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import org.bson.BsonDocument;

/**
 * A NumberFacetFieldDefinition defines how mongot should index numbers for faceting on a single
 * field.
 */
public record NumberFacetFieldDefinition(NumericFieldOptions options)
    implements FieldTypeDefinition, NumericFieldDefinition {

  /**
   * A NumberFacetFieldDefinition defines how numbers are indexed for faceting on a field.
   *
   * @param options the numeric options to use in indexing.
   */
  public NumberFacetFieldDefinition {}

  static NumberFacetFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new NumberFacetFieldDefinition(NumericFieldOptions.fromBson(parser));
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
    return Type.NUMBER_FACET;
  }
}
