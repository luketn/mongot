package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Objects;
import java.util.Optional;
import org.bson.BsonDocument;

/** The TokenFieldDefinition class is used to define the options of the token field type. */
public record TokenFieldDefinition(Optional<StockNormalizerName> normalizer)
    implements FieldTypeDefinition, FacetableStringFieldDefinition {
  public static class Fields {
    static final Field.Optional<StockNormalizerName> NORMALIZER =
        Field.builder("normalizer")
            .enumField(StockNormalizerName.class)
            .asCamelCase()
            .optional()
            .noDefault();
  }

  static TokenFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new TokenFieldDefinition(parser.getField(Fields.NORMALIZER).unwrap());
  }

  @Override
  public Type getType() {
    return Type.TOKEN;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    return BsonDocumentBuilder.builder().field(Fields.NORMALIZER, this.normalizer).build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TokenFieldDefinition)) {
      return false;
    }
    TokenFieldDefinition that = (TokenFieldDefinition) o;
    return Objects.equals(this.normalizer, that.normalizer);
  }
}
