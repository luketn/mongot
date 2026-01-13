package com.xgen.mongot.index.definition;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.bson.BsonDocument;

/**
 * StringFieldDefinition defines how to index a string value for a field by passing the value
 * through an Analyzer.
 */
public record StringFieldDefinition(
    Optional<String> analyzerName,
    Optional<String> searchAnalyzerName,
    Optional<SimilarityDefinition> similarity,
    Optional<Integer> ignoreAbove,
    IndexOptions indexOptions,
    boolean storeFlag,
    NormsOptions norms,
    ImmutableMap<String, StringFieldDefinition> multi)
    implements FieldTypeDefinition {

  public static class Fields {

    static final Field.Optional<String> ANALYZER =
        Field.builder("analyzer").stringField().mustNotBeEmpty().optional().noDefault();

    static final Field.Optional<String> SEARCH_ANALYZER =
        Field.builder("searchAnalyzer").stringField().mustNotBeEmpty().optional().noDefault();

    static final Field.Optional<SimilarityDefinition> SIMILARITY =
        Field.builder("similarity")
            .classField(SimilarityDefinition::fromBson, SimilarityDefinition::toBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<Integer> IGNORE_ABOVE =
        Field.builder("ignoreAbove").intField().mustBePositive().optional().noDefault();

    public static final Field.WithDefault<IndexOptions> INDEX_OPTIONS =
        Field.builder("indexOptions")
            .enumField(IndexOptions.class)
            .asCamelCase()
            .optional()
            .withDefault(IndexOptions.OFFSETS);

    public static final Field.WithDefault<Boolean> STORE =
        Field.builder("store").booleanField().optional().withDefault(true);

    public static final Field.WithDefault<NormsOptions> NORMS =
        Field.builder("norms")
            .enumField(NormsOptions.class)
            .asCamelCase()
            .optional()
            .withDefault(NormsOptions.INCLUDE);

    static final Field.WithDefault<Map<String, StringFieldDefinition>> MULTI =
        Field.builder("multi")
            /*
             * Multi fields require the "type" field, which StringFieldDefinition::fromBson will not
             * expect.
             *
             * <p>multiFromBson() checks that the "type" field is included, and encoding the
             * StringFieldDefinition using FieldTypeDefinition::toBson will ensure that the "type"
             * field is included when serializing.
             */
            .classField(StringFieldDefinition::multiFromBson, FieldTypeDefinition::toBson)
            .disallowUnknownFields()
            .asMap()
            .validateKeys(
                key ->
                    key.contains(".")
                        ? Optional.of("multi path cannot contain \".\"")
                        : Optional.empty())
            .optional()
            .withDefault(Collections.emptyMap());
  }

  public enum IndexOptions {
    DOCS,
    FREQS,
    POSITIONS,
    OFFSETS;

    public boolean containsPositionInfo() {
      return this == POSITIONS || this == OFFSETS;
    }
  }

  public enum NormsOptions {
    INCLUDE,
    OMIT
  }

  public static StringFieldDefinition create(
      Optional<String> analyzerName,
      Optional<String> searchAnalyzerName,
      Optional<SimilarityDefinition> similarity,
      Optional<Integer> ignoreAbove,
      IndexOptions indexOptions,
      boolean store,
      NormsOptions norms,
      Map<String, StringFieldDefinition> multi) {
    return new StringFieldDefinition(
        analyzerName,
        searchAnalyzerName,
        similarity,
        ignoreAbove,
        indexOptions,
        store,
        norms,
        ImmutableMap.copyOf(multi));
  }

  static StringFieldDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return StringFieldDefinition.create(
        parser.getField(Fields.ANALYZER).unwrap(),
        parser.getField(Fields.SEARCH_ANALYZER).unwrap(),
        parser.getField(Fields.SIMILARITY).unwrap(),
        parser.getField(Fields.IGNORE_ABOVE).unwrap(),
        parser.getField(Fields.INDEX_OPTIONS).unwrap(),
        parser.getField(Fields.STORE).unwrap(),
        parser.getField(Fields.NORMS).unwrap(),
        parser.getField(Fields.MULTI).unwrap());
  }

  private static StringFieldDefinition multiFromBson(DocumentParser parser)
      throws BsonParseException {
    Type type = parser.getField(FieldTypeDefinition.Fields.TYPE).unwrap();
    if (type != Type.STRING) {
      parser.getContext().handleSemanticError("multi definition must be string field");
    }

    StringFieldDefinition definition = fromBson(parser);
    if (!definition.multi().isEmpty()) {
      parser
          .getContext()
          .handleSemanticError("multi definition cannot contain nested multi definition");
    }

    return definition;
  }

  @Override
  public BsonDocument fieldTypeToBson() {
    BsonDocumentBuilder builder =
        BsonDocumentBuilder.builder()
            .field(Fields.ANALYZER, this.analyzerName)
            .field(Fields.SEARCH_ANALYZER, this.searchAnalyzerName)
            .field(Fields.SIMILARITY, this.similarity)
            .field(Fields.IGNORE_ABOVE, this.ignoreAbove)
            .field(Fields.INDEX_OPTIONS, this.indexOptions)
            .field(Fields.STORE, this.storeFlag)
            .field(Fields.NORMS, this.norms);

    // Explicitly only serialize multi if there is a multi present.
    // This will prevent a multi definition from serializing with an empty multi map.
    if (!this.multi().isEmpty()) {
      builder.field(Fields.MULTI, this.multi);
    }

    return builder.build();
  }

  /**
   * An Optional friendly wrapper for finding multi fields in a hashmap.
   *
   * @param multiName name of multi to find
   * @return FieldDefinition, or Optional if not found
   */
  Optional<StringFieldDefinition> getMultiDefinition(String multiName) {
    if (this.multi.containsKey(multiName)) {
      return Optional.of(this.multi.get(multiName));
    }
    return Optional.empty();
  }

  @Override
  public Type getType() {
    return Type.STRING;
  }
}
