package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Objects;
import org.bson.BsonDocument;

/**
 * Represents a 'vector' field definition within a search index.
 *
 * <p>This class basically wraps a {@link VectorFieldSpecification} to provide feature parity with
 * vector index 'vector' data field type.
 */
public record SearchIndexVectorFieldDefinition(VectorFieldSpecification specification)
    implements FieldTypeDefinition {

  @Override
  public Type getType() {
    return Type.VECTOR;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return this.specification.toBson();
  }

  static SearchIndexVectorFieldDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new SearchIndexVectorFieldDefinition(VectorFieldSpecification.fromBson(parser));
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SearchIndexVectorFieldDefinition that = (SearchIndexVectorFieldDefinition) o;
    return Objects.equals(this.specification, that.specification);
  }
}
