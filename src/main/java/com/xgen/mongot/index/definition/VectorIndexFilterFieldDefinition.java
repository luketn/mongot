package com.xgen.mongot.index.definition;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import java.util.Objects;
import org.bson.BsonDocument;

/** Part of a Vector Index definition that represents a path that can be used as a query filter. */
public class VectorIndexFilterFieldDefinition extends VectorIndexFieldDefinition {
  public VectorIndexFilterFieldDefinition(FieldPath path) {
    super(path);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(VectorIndexFieldDefinition.Fields.TYPE, getType())
        .field(VectorIndexFieldDefinition.Fields.PATH, this.path)
        .build();
  }

  public static VectorIndexFilterFieldDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new VectorIndexFilterFieldDefinition(
        parser.getField(VectorIndexFieldDefinition.Fields.PATH).unwrap());
  }

  @VisibleForTesting
  public static VectorIndexFilterFieldDefinition create(FieldPath path) {
    return new VectorIndexFilterFieldDefinition(path);
  }

  @Override
  public Type getType() {
    return Type.FILTER;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof VectorIndexFilterFieldDefinition)) {
      return false;
    }
    VectorIndexFilterFieldDefinition that = (VectorIndexFilterFieldDefinition) o;
    return Objects.equals(this.path, that.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.path);
  }
}
