package com.xgen.mongot.server.command.management.definition;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public sealed interface SearchIndexCommandDefinition extends DocumentEncodable
    permits CreateSearchIndexesCommandDefinition,
        DropSearchIndexCommandDefinition,
        ListSearchIndexesCommandDefinition,
        UpdateSearchIndexCommandDefinition {
  static class Fields {
    static final Field.Optional<String> CREATE_SEARCH_INDEXES =
        Field.builder("createSearchIndexes").stringField().optional().noDefault();

    static final Field.Optional<String> LIST_SEARCH_INDEXES =
        Field.builder("listSearchIndexes").stringField().optional().noDefault();

    // This BsonDocument field is just used to check for presence of the key.
    // If present, it will be deserialized to a ListSearchIndexesCommandDefinition.
    static final Field.Optional<BsonDocument> LIST_SEARCH_INDEXES_AGG =
        Field.builder("$listSearchIndexes").documentField().optional().noDefault();

    static final Field.Optional<String> UPDATE_SEARCH_INDEX =
        Field.builder("updateSearchIndex").stringField().optional().noDefault();

    static final Field.Optional<String> DROP_SEARCH_INDEX =
        Field.builder("dropSearchIndex").stringField().optional().noDefault();
  }

  static SearchIndexCommandDefinition fromBson(DocumentParser parser) throws BsonParseException {
    // Validate only one command type is present
    parser
        .getGroup()
        .exactlyOneOf(
            parser.getField(Fields.CREATE_SEARCH_INDEXES),
            parser.getField(Fields.LIST_SEARCH_INDEXES),
            parser.getField(Fields.LIST_SEARCH_INDEXES_AGG),
            parser.getField(Fields.UPDATE_SEARCH_INDEX),
            parser.getField(Fields.DROP_SEARCH_INDEX));

    if (parser.getField(Fields.CREATE_SEARCH_INDEXES).unwrap().isPresent()) {
      return CreateSearchIndexesCommandDefinition.fromBson(parser);
    } else if (parser.getField(Fields.LIST_SEARCH_INDEXES).unwrap().isPresent()) {
      return ListSearchIndexesCommandDefinition.fromBson(parser);
    } else if (parser.getField(Fields.LIST_SEARCH_INDEXES_AGG).unwrap().isPresent()) {
      return ListSearchIndexesCommandDefinition.fromBson(parser);
    } else if (parser.getField(Fields.UPDATE_SEARCH_INDEX).unwrap().isPresent()) {
      return UpdateSearchIndexCommandDefinition.fromBson(parser);
    } else if (parser.getField(Fields.DROP_SEARCH_INDEX).unwrap().isPresent()) {
      return DropSearchIndexCommandDefinition.fromBson(parser);
    }
    return Check.unreachable("No valid search index command field found in BSON");
  }
}
