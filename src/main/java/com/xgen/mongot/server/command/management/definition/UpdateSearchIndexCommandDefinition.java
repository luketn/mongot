package com.xgen.mongot.server.command.management.definition;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.server.command.management.definition.common.UserIndexDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record UpdateSearchIndexCommandDefinition(
    String collectionName,
    Optional<ObjectId> id,
    Optional<String> name,
    Optional<IndexDefinition.Type> type,
    BsonDocument definitionBson)
    implements SearchIndexCommandDefinition {

  static class Fields {
    static final Field.Required<String> UPDATE_SEARCH_INDEX =
        Field.builder("updateSearchIndex").stringField().required();

    static final Field.Optional<ObjectId> ID =
        Field.builder("id").objectIdField().encodeAsString().optional().noDefault();

    static final Field.Optional<String> NAME =
        Field.builder("name").stringField().optional().noDefault();

    static final Field.Optional<IndexDefinition.Type> TYPE =
        Field.builder("type")
            .enumField(IndexDefinition.Type.class)
            .asCamelCase()
            .optional()
            .noDefault();

    static final Field.Required<BsonDocument> DEFINITION =
        Field.builder("definition").documentField().required();
  }

  public static final String NAME = "updateSearchIndex";

  public static UpdateSearchIndexCommandDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    // Update commands must have the index's id or name, but not both.
    parser.getGroup().exactlyOneOf(parser.getField(Fields.ID), parser.getField(Fields.NAME));

    Optional<IndexDefinition.Type> type = parser.getField(Fields.TYPE).unwrap();
    BsonDocument definitionBson = parser.getField(Fields.DEFINITION).unwrap();

    return new UpdateSearchIndexCommandDefinition(
        parser.getField(Fields.UPDATE_SEARCH_INDEX).unwrap(),
        parser.getField(Fields.ID).unwrap(),
        parser.getField(Fields.NAME).unwrap(),
        type,
        definitionBson);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.UPDATE_SEARCH_INDEX, this.collectionName)
        .field(Fields.ID, this.id)
        .field(Fields.NAME, this.name)
        .field(Fields.TYPE, this.type)
        .field(Fields.DEFINITION, this.definitionBson)
        .build();
  }

  public UserIndexDefinition indexDefinition(IndexDefinition.Type inferredType)
      throws BsonParseException {
    IndexDefinition.Type type = this.type.orElse(inferredType);
    try (var parser =
        BsonDocumentParser.fromRoot(this.definitionBson).allowUnknownFields(false).build()) {
      return UserIndexDefinition.fromBson(parser, type);
    }
  }
}
