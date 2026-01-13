package com.xgen.mongot.server.command.management.definition;

import com.xgen.mongot.server.command.management.definition.common.UserViewDefinition;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Encodable;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import java.util.UUID;
import org.bson.BsonDocument;

/**
 * Mongod sends all Search Index commands (create, list, update, drop) as a generic
 * manageSearchIndex command. We will tell them apart by looking in its userCommand document for one
 * of the four expected keys.
 */
public record ManageSearchIndexCommandDefinition(
    String db,
    String collectionName,
    UUID collectionUuid,
    Optional<UserViewDefinition> view,
    SearchIndexCommandDefinition userCommand)
    implements Encodable {

  static class Fields {
    static final Field.Required<String> MANAGE_SEARCH_INDEX =
        Field.builder("manageSearchIndex").stringField().required();

    static final Field.Required<UUID> COLLECTION_UUID =
        Field.builder("collectionUUID").uuidField().encodeAsString().required();

    static final Field.Optional<UserViewDefinition> VIEW =
        Field.builder("view")
            .classField(UserViewDefinition::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();

    static final Field.Required<String> DB = Field.builder("$db").stringField().required();

    static final Field.Required<SearchIndexCommandDefinition> USER_COMMAND =
        Field.builder("userCommand")
            .classField(SearchIndexCommandDefinition::fromBson)
            .disallowUnknownFields()
            .required();
  }

  public static final String NAME = "manageSearchIndex";

  public static ManageSearchIndexCommandDefinition fromBson(DocumentParser parser)
      throws BsonParseException {

    SearchIndexCommandDefinition command = parser.getField(Fields.USER_COMMAND).unwrap();
    Optional<UserViewDefinition> view = parser.getField(Fields.VIEW).unwrap();

    if (command instanceof CreateSearchIndexesCommandDefinition
        && view.isPresent()
        && view.get().effectivePipeline().isEmpty()) {
      parser.getContext().handleSemanticError("View pipeline is required for the create operation");
    }

    return new ManageSearchIndexCommandDefinition(
        parser.getField(Fields.DB).unwrap(),
        parser.getField(Fields.MANAGE_SEARCH_INDEX).unwrap(),
        parser.getField(Fields.COLLECTION_UUID).unwrap(),
        view,
        command);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DB, this.db)
        .field(Fields.MANAGE_SEARCH_INDEX, this.collectionName)
        .field(Fields.COLLECTION_UUID, this.collectionUuid)
        .field(Fields.VIEW, this.view)
        .field(Fields.USER_COMMAND, this.userCommand)
        .build();
  }
}
