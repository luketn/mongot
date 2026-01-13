package com.xgen.mongot.server.command.search.definition.request;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;

public record SearchCommandDefinition(
    String db,
    String collectionName,
    UUID collectionUuid,
    Optional<String> viewName,
    BsonDocument queryDocument,
    Optional<ExplainDefinition> explain,
    Optional<Integer> intermediateVersion,
    Optional<CursorOptionsDefinition> cursorOptions,
    Optional<OptimizationFlagsDefinition> optimizationFlags)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Optional<String> SEARCH =
        Field.builder("search").stringField().optional().noDefault();

    static final Field.Optional<String> SEARCH_BETA =
        Field.builder("searchBeta").stringField().optional().noDefault();

    static final Field.Required<String> DB = Field.builder("$db").stringField().required();

    static final Field.Required<UUID> COLLECTION_UUID =
        Field.builder("collectionUUID").uuidField().required();

    static final Field.Optional<String> VIEW_NAME =
        Field.builder("viewName").stringField().optional().noDefault();

    static final Field.Required<BsonDocument> QUERY =
        Field.builder("query").documentField().required();

    static final Field.Optional<ExplainDefinition> EXPLAIN =
        Field.builder("explain")
            .classField(ExplainDefinition::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<Integer> INTERMEDIATE_VERSION =
        Field.builder("intermediate").intField().optional().noDefault();

    static final Field.Optional<CursorOptionsDefinition> CURSOR_OPTIONS =
        Field.builder("cursorOptions")
            .classField(CursorOptionsDefinition::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Optional<OptimizationFlagsDefinition> OPTIMIZATION_FLAGS =
        Field.builder("optimizationFlags")
            .classField(OptimizationFlagsDefinition::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  public static final String NAME = "search";
  public static final String SEARCH_BETA_LEGACY_NAME = "searchBeta";

  public static SearchCommandDefinition fromBson(DocumentParser parser) throws BsonParseException {
    // Allow both search and searchBeta keys for now.
    var search = parser.getField(Fields.SEARCH);
    var searchBeta = parser.getField(Fields.SEARCH_BETA);
    var collectionName = parser.getGroup().exactlyOneOf(search, searchBeta);

    return new SearchCommandDefinition(
        parser.getField(Fields.DB).unwrap(),
        collectionName,
        parser.getField(Fields.COLLECTION_UUID).unwrap(),
        parser.getField(Fields.VIEW_NAME).unwrap(),
        parser.getField(Fields.QUERY).unwrap(),
        parser.getField(Fields.EXPLAIN).unwrap(),
        parser.getField(Fields.INTERMEDIATE_VERSION).unwrap(),
        parser.getField(Fields.CURSOR_OPTIONS).unwrap(),
        parser.getField(Fields.OPTIMIZATION_FLAGS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        // Fields.SEARCH must be the first field in the BsonDocument
        .field(Fields.SEARCH, Optional.of(this.collectionName))
        .field(Fields.DB, this.db)
        .field(Fields.COLLECTION_UUID, this.collectionUuid)
        .field(Fields.QUERY, this.queryDocument)
        .field(Fields.EXPLAIN, this.explain)
        .field(Fields.INTERMEDIATE_VERSION, this.intermediateVersion)
        .field(Fields.CURSOR_OPTIONS, this.cursorOptions)
        .build();
  }
}
