package com.xgen.mongot.index.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record NumericFieldOptions(
    Representation representation, boolean indexDoubles, boolean indexIntegers) {
  public static class Fields {
    public static final Field.WithDefault<Representation> REPRESENTATION =
        Field.builder("representation")
            .enumField(Representation.class)
            .asCamelCase()
            .optional()
            .withDefault(Representation.DOUBLE);

    public static final Field.WithDefault<Boolean> INDEX_DOUBLES =
        Field.builder("indexDoubles").booleanField().optional().withDefault(true);

    public static final Field.WithDefault<Boolean> INDEX_INTEGERS =
        Field.builder("indexIntegers").booleanField().optional().withDefault(true);
  }

  public enum Representation {
    INT64,
    DOUBLE
  }

  /**
   * A NumericFieldOptions defines how numbers are indexed for a field.
   *
   * @param representation the numeric representation to use in indexing.
   * @param indexDoubles true if doubles should be indexed for this field.
   * @param indexIntegers true if integers should be indexed for this field.
   */
  public NumericFieldOptions {}

  static NumericFieldOptions fromBson(DocumentParser parser) throws BsonParseException {
    var indexDoubles = parser.getField(Fields.INDEX_DOUBLES).unwrap();
    var indexIntegers = parser.getField(Fields.INDEX_INTEGERS).unwrap();
    if (!indexDoubles && !indexIntegers) {
      parser
          .getContext()
          .handleSemanticError("indexDoubles and indexIntegers may not both be false");
    }

    return new NumericFieldOptions(
        parser.getField(Fields.REPRESENTATION).unwrap(), indexDoubles, indexIntegers);
  }

  BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.REPRESENTATION, this.representation)
        .field(Fields.INDEX_DOUBLES, this.indexDoubles)
        .field(Fields.INDEX_INTEGERS, this.indexIntegers)
        .build();
  }
}
