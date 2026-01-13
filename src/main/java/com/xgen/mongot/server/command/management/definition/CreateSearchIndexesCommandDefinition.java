package com.xgen.mongot.server.command.management.definition;

import com.xgen.mongot.server.command.management.definition.common.CommonDefinitions;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndex;
import com.xgen.mongot.server.command.management.definition.common.NamedSearchIndexId;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;

public record CreateSearchIndexesCommandDefinition(
    String collectionName, List<NamedSearchIndex> indexes) implements SearchIndexCommandDefinition {

  static class Fields {
    static final Field.Required<String> CREATE_SEARCH_INDEXES =
        Field.builder("createSearchIndexes").stringField().required();

    static final Field.Required<List<NamedSearchIndex>> INDEXES =
        Field.builder("indexes")
            .classField(NamedSearchIndex::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  public static class CreateSearchIndexesResponse implements DocumentEncodable {
    public static class Fields {
      public static final Field.Required<Integer> OK =
          Field.builder("ok").intField().mustBeWithinBounds(Range.of(0, 1)).required();

      public static final Field.Required<List<NamedSearchIndexId>> INDEXES_CREATED =
          Field.builder("indexesCreated")
              .classField(NamedSearchIndexId::fromBson)
              .disallowUnknownFields()
              .asList()
              .required();
    }

    private final Integer ok = CommonDefinitions.OK_SUCCESS_CODE;
    private final List<NamedSearchIndexId> indexesCreated;

    public CreateSearchIndexesResponse(List<NamedSearchIndexId> indexesCreated) {
      this.indexesCreated = indexesCreated;
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.OK, this.ok)
          .field(Fields.INDEXES_CREATED, this.indexesCreated)
          .build();
    }

    public static CreateSearchIndexesResponse fromBson(DocumentParser parser)
        throws BsonParseException {
      return new CreateSearchIndexesResponse(parser.getField(Fields.INDEXES_CREATED).unwrap());
    }
  }

  public static final String NAME = "createSearchIndexes";

  public static CreateSearchIndexesCommandDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new CreateSearchIndexesCommandDefinition(
        parser.getField(Fields.CREATE_SEARCH_INDEXES).unwrap(),
        parser.getField(Fields.INDEXES).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.CREATE_SEARCH_INDEXES, this.collectionName)
        .field(Fields.INDEXES, this.indexes)
        .build();
  }
}
