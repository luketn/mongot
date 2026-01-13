package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonDocumentParser;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

public record CursorOptionsDefinition(
    Optional<Integer> docsRequested, Optional<Integer> batchSize, boolean requireSequenceTokens)
    implements DocumentEncodable {

  private static class Fields {
    private static final Field.Optional<Integer> DOCS_REQUESTED =
        Field.builder("docsRequested").intField().mustBeNonNegative().optional().noDefault();
    private static final Field.Optional<Integer> BATCH_SIZE =
        Field.builder("batchSize").intField().mustBeNonNegative().optional().noDefault();

    /** If present, Mongot should compute pagination tokens for every SearchResult. */
    private static final Field.WithDefault<Boolean> REQUIRE_SEQUENCE_TOKENS =
        Field.builder("requiresSearchSequenceToken").booleanField().optional().withDefault(false);
  }

  public CursorOptionsDefinition(
      Optional<Integer> docsRequested, Optional<Integer> batchSize, boolean requireSequenceTokens) {
    checkArg(
        !(docsRequested.isPresent() && batchSize.isPresent()),
        "docsRequested and batchSize may not both be specified");
    docsRequested.ifPresent(val -> Check.argNotNegative(val, "docsRequested"));
    this.docsRequested = docsRequested;
    batchSize.ifPresent(val -> Check.argIsPositive(val, "batchSize"));
    this.batchSize = batchSize;
    this.requireSequenceTokens = requireSequenceTokens;
  }

  public static CursorOptionsDefinition fromBson(BsonDocument document) throws BsonParseException {
    try (var parser = BsonDocumentParser.fromRoot(document).build()) {
      return fromBson(parser);
    }
  }

  public static CursorOptionsDefinition fromBson(DocumentParser parser) throws BsonParseException {
    return new CursorOptionsDefinition(
        parser.getField(Fields.DOCS_REQUESTED).unwrap(),
        parser.getField(Fields.BATCH_SIZE).unwrap(),
        parser.getField(Fields.REQUIRE_SEQUENCE_TOKENS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DOCS_REQUESTED, this.docsRequested)
        .field(Fields.BATCH_SIZE, this.batchSize)
        .field(Fields.REQUIRE_SEQUENCE_TOKENS, this.requireSequenceTokens)
        .build();
  }

  public QueryCursorOptions toQueryCursorOptions() {
    return new QueryCursorOptions(this.docsRequested, this.batchSize, this.requireSequenceTokens);
  }
}
