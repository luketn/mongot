package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Objects;
import org.bson.BsonDocument;

/** Part of a Vector Index definition that represents a field with an array of vectors. */
public class VectorDataFieldDefinition extends VectorIndexVectorFieldDefinition {

  private final VectorFieldSpecification specification;

  public VectorDataFieldDefinition(FieldPath path, VectorFieldSpecification specification) {
    super(path);
    this.specification = specification;
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(VectorIndexFieldDefinition.Fields.TYPE, getType())
        .field(VectorIndexFieldDefinition.Fields.PATH, this.path)
        .join(this.specification.toBson())
        .build();
  }

  public static VectorDataFieldDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    FieldPath path = parser.getField(VectorIndexFieldDefinition.Fields.PATH).unwrap();

    return new VectorDataFieldDefinition(path, VectorFieldSpecification.fromBson(parser));
  }

  @Override
  public VectorFieldSpecification specification() {
    return this.specification;
  }

  @Override
  public Type getType() {
    return Type.VECTOR;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorDataFieldDefinition)) {
      return false;
    }
    VectorDataFieldDefinition that = (VectorDataFieldDefinition) o;
    return Objects.equals(this.path, that.path)
        && Objects.equals(this.specification, that.specification);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.path, this.specification);
  }
}
