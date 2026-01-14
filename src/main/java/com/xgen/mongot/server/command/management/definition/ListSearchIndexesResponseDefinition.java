package com.xgen.mongot.server.command.management.definition;

import static com.xgen.mongot.cursor.serialization.MongotCursorResult.EXHAUSTED_CURSOR_ID;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.server.command.management.util.IndexMapper;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.Range;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record ListSearchIndexesResponseDefinition(int ok, Cursor cursor)
    implements DocumentEncodable {

  static class Fields {
    static final Field.Required<Integer> OK =
        Field.builder("ok").intField().mustBeWithinBounds(Range.is(1)).required();
    static final Field.Required<Cursor> CURSOR =
        Field.builder("cursor").classField(Cursor::fromBson).disallowUnknownFields().required();
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.OK, this.ok)
        .field(Fields.CURSOR, this.cursor)
        .build();
  }

  public static ListSearchIndexesResponseDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new ListSearchIndexesResponseDefinition(
        parser.getField(Fields.OK).unwrap(), parser.getField(Fields.CURSOR).unwrap());
  }

  public record Cursor(String namespace, List<BsonDocument> firstBatch)
      implements DocumentEncodable {
    // A cursor ID of 0 is hardcoded because this response explicitly does not implement cursors.
    // An IndexInformationTooLarge error will be returned if the response is larger than a single
    // batch.
    private static final int ID = EXHAUSTED_CURSOR_ID;

    public static class Fields {

      public static final Field.Required<Integer> ID =
          Field.builder("id")
              .intField()
              .mustBeWithinBounds(Range.is(EXHAUSTED_CURSOR_ID))
              .required();
      public static final Field.Required<String> NS = Field.builder("ns").stringField().required();

      // TODO(CLOUDP-280897): Use a type for index definitions
      public static final Field.Required<List<BsonDocument>> FIRST_BATCH =
          Field.builder("firstBatch").documentField().asList().required();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.ID, ID)
          .field(Fields.NS, this.namespace)
          .field(Fields.FIRST_BATCH, this.firstBatch)
          .build();
    }

    public static Cursor fromBson(DocumentParser parser) throws BsonParseException {
      return new Cursor(
          parser.getField(Fields.NS).unwrap(), parser.getField(Fields.FIRST_BATCH).unwrap());
    }
  }

  public record IndexEntry(
      ObjectId indexId,
      String name,
      String type,
      Optional<String> status,
      Optional<Long> numDocs,
      IndexDefinition latestDefinition)
      implements DocumentEncodable {
    private static class Fields {
      private static final Field.Required<ObjectId> ID =
          Field.builder("id").objectIdField().encodeAsString().required();

      private static final Field.Required<String> NAME =
          Field.builder("name").stringField().required();

      private static final Field.Required<String> TYPE =
          Field.builder("type").stringField().required();

      // In Public Preview this field is only populated for internal e2e testing.
      private static final Field.Optional<String> STATUS =
          Field.builder("status").stringField().optional().noDefault();

      // In Public Preview this field is only populated for internal e2e testing.
      private static final Field.Optional<Long> NUM_DOCS =
          Field.builder("numDocs").longField().optional().noDefault();

      private static final Field.Required<IndexDefinition> LATEST_DEFINITION =
          Field.builder("latestDefinition")
              .classField(IndexDefinition::fromBson)
              .disallowUnknownFields()
              .required();
    }

    @Override
    public BsonDocument toBson() {
      return BsonDocumentBuilder.builder()
          .field(Fields.ID, this.indexId)
          .field(Fields.NAME, this.name)
          .field(Fields.TYPE, this.type)
          .field(Fields.STATUS, this.status)
          .field(Fields.NUM_DOCS, this.numDocs)
          .field(Fields.LATEST_DEFINITION, this.latestDefinition)
          .build();
    }

    public static IndexEntry fromIndexDefinition(IndexDefinition indexDefinition) {
      return new IndexEntry(
          indexDefinition.getIndexId(),
          indexDefinition.getName(),
          IndexMapper.toExternalType(indexDefinition.getType()),
          Optional.empty(),
          Optional.empty(),
          indexDefinition);
    }

    public static IndexEntry fromIndexDefinition(
        IndexDefinition indexDefinition, IndexStatus.StatusCode indexStatusCode, long numDocs) {
      return new IndexEntry(
          indexDefinition.getIndexId(),
          indexDefinition.getName(),
          IndexMapper.toExternalType(indexDefinition.getType()),
          Optional.of(IndexMapper.toExternalStatus(indexStatusCode)),
          Optional.of(numDocs),
          indexDefinition);
    }
  }
}
