package com.xgen.mongot.server.command.management.definition;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import com.xgen.mongot.util.bson.parser.ParsedField;
import java.util.Optional;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

/**
 * The shape of the list command differs slightly depending on its context, but we normalize it
 * during parsing.
 *
 * <p>If used as a stage in an aggregation pipeline, the name/id is nested under $listSearchIndexes.
 *
 * <pre>
 * {
 *   "$db": "myDatabase",
 *   "manageSearchIndex": "myCollection",
 *   "userCommand": {
 *     "$listSearchIndexes": {
 *       "name": "myIndex",
 *       "id": "myID"
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>If used as a standalone command, the name/id is at the same level as listSearchIndexes.
 *
 * <pre>
 * {
 *   "$db": "myDatabase",
 *   "manageSearchIndex": "myCollection",
 *   "userCommand": {
 *     "listSearchIndexes": "myCollection",
 *     "name": "myIndex",
 *     "id": "myID"
 *   }
 * }
 * </pre>
 */
public record ListSearchIndexesCommandDefinition(ListTarget target)
    implements SearchIndexCommandDefinition {

  static class Fields {
    static final Field.Optional<ListTarget> LIST_SEARCH_AGG =
        Field.builder("$listSearchIndexes")
            .classField(ListTarget::fromBson)
            .disallowUnknownFields()
            .optional()
            .noDefault();
  }

  /**
   * We can do a list of either a single index, or all of them.
   *
   * <ul>
   *   <li>For a single index, either ID or name should be provided, but not both.
   *   <li>For all indexes, neither field should be provided.
   * </ul>
   */
  public record ListTarget(Optional<ObjectId> indexId, Optional<String> indexName)
      implements DocumentEncodable {
    private static class Fields {
      static final Field.Optional<ObjectId> INDEX_ID =
          Field.builder("id").objectIdField().encodeAsString().optional().noDefault();

      static final Field.Optional<String> INDEX_NAME =
          Field.builder("name").stringField().optional().noDefault();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.INDEX_ID, this.indexId)
          .field(Fields.INDEX_NAME, this.indexName)
          .build();
    }

    public static ListTarget fromBson(DocumentParser parser) throws BsonParseException {
      ParsedField.Optional<ObjectId> id = parser.getField(Fields.INDEX_ID);
      ParsedField.Optional<String> name = parser.getField(Fields.INDEX_NAME);

      parser.getGroup().atMostOneOf(id, name);

      return new ListTarget(id.unwrap(), name.unwrap());
    }
  }

  public static final String NAME = "listSearchIndexes";

  public static ListSearchIndexesCommandDefinition fromBson(DocumentParser parser)
      throws BsonParseException {

    // If this command is part of an aggregation pipeline, the target will be
    ListTarget target =
        parser
            .getField(Fields.LIST_SEARCH_AGG)
            .unwrap()
            .orElse(
                new ListTarget(
                    parser.getField(ListTarget.Fields.INDEX_ID).unwrap(),
                    parser.getField(ListTarget.Fields.INDEX_NAME).unwrap()));

    return new ListSearchIndexesCommandDefinition(target);
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.LIST_SEARCH_AGG, Optional.of(this.target))
        .build();
  }
}
