package com.xgen.mongot.index.definition.config;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * Represents configuration specific to Atlas Search index definitions that is unrelated to Lucene.
 * An {@link IndexDefinitionConfig} may contain parameters that configure behavior of index
 * definitions (e.g. index definition specific limits).
 */
public record IndexDefinitionConfig(Optional<Integer> maxEmbeddedDocumentsNestingLevel)
    implements DocumentEncodable {

  private static final int DEFAULT_MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL = 5;

  static class Fields {
    static final Field.Optional<Integer> MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL =
        Field.builder("maxEmbeddedDocumentsNestingLevel")
            .intField()
            .mustBeNonNegative()
            .optional()
            .noDefault();
  }

  public static IndexDefinitionConfig create(Optional<Integer> maxEmbeddedDocumentsNestingLevel) {
    return new IndexDefinitionConfig(maxEmbeddedDocumentsNestingLevel);
  }

  public static IndexDefinitionConfig getDefault() {
    return create(Optional.empty());
  }

  public int getMaxEmbeddedDocumentsNestingLevel() {
    return this.maxEmbeddedDocumentsNestingLevel.orElse(
        DEFAULT_MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL);
  }

  public static IndexDefinitionConfig fromBson(DocumentParser parser) throws BsonParseException {
    return new IndexDefinitionConfig(
        parser.getField(Fields.MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MAX_EMBEDDED_DOCUMENTS_NESTING_LEVEL, this.maxEmbeddedDocumentsNestingLevel)
        .build();
  }
}
