package com.xgen.mongot.server.command.management.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record DropSearchIndexCommandDefinition(
    String collectionName, Optional<ObjectId> id, Optional<String> name)
    implements SearchIndexCommandDefinition {

  static class Fields {
    static final Field.Required<String> DROP_SEARCH_INDEXES =
        Field.builder("dropSearchIndex").stringField().required();

    static final Field.Optional<ObjectId> ID =
        Field.builder("id").objectIdField().encodeAsString().optional().noDefault();

    static final Field.Optional<String> NAME =
        Field.builder("name").stringField().optional().noDefault();
  }

  public static final String NAME = "dropSearchIndex";

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DROP_SEARCH_INDEXES, this.collectionName)
        .field(Fields.ID, this.id)
        .field(Fields.NAME, this.name)
        .build();
  }

  public static DropSearchIndexCommandDefinition fromBson(DocumentParser parser)
      throws BsonParseException {

    // The index must be identified by name or ID, but not both.
    ParsedField.Optional<ObjectId> indexIdField = parser.getField(Fields.ID);
    ParsedField.Optional<String> indexNameField = parser.getField(Fields.NAME);
    parser.getGroup().exactlyOneOf(indexIdField, indexNameField);

    return new DropSearchIndexCommandDefinition(
        parser.getField(Fields.DROP_SEARCH_INDEXES).unwrap(),
        indexIdField.unwrap(),
        indexNameField.unwrap());
  }
}
