package com.xgen.mongot.server.command.search.definition.request;

import static com.xgen.mongot.util.Check.checkArg;

import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

public record BatchOptionsDefinition(Optional<Integer> docsRequested, Optional<Integer> batchSize)
    implements DocumentEncodable {

  private static class Fields {
    private static final Field.Optional<Integer> DOCS_REQUESTED =
        Field.builder("docsRequested").intField().mustBeNonNegative().optional().noDefault();
    private static final Field.Optional<Integer> BATCH_SIZE =
        Field.builder("batchSize").intField().mustBeNonNegative().optional().noDefault();
  }

  public BatchOptionsDefinition(Optional<Integer> docsRequested, Optional<Integer> batchSize) {
    checkArg(
        !(docsRequested.isPresent() && batchSize.isPresent()),
        "docsRequested and batchSize may not both be specified");
    docsRequested.ifPresent(val -> Check.argNotNegative(val, "docsRequested"));
    this.docsRequested = docsRequested;
    batchSize.ifPresent(val -> Check.argNotNegative(val, "batchSize"));
    this.batchSize = batchSize;
  }

  public static BatchOptionsDefinition fromBson(DocumentParser parser) throws BsonParseException {
    var docsRequested = parser.getField(Fields.DOCS_REQUESTED);
    var batchSize = parser.getField(Fields.BATCH_SIZE);
    parser.getGroup().atMostOneOf(docsRequested, batchSize);
    return new BatchOptionsDefinition(docsRequested.unwrap(), batchSize.unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.DOCS_REQUESTED, this.docsRequested)
        .field(Fields.BATCH_SIZE, this.batchSize)
        .build();
  }

  public BatchCursorOptions toQueryCursorOptions() {
    return new BatchCursorOptions(this.docsRequested, this.batchSize);
  }
}
