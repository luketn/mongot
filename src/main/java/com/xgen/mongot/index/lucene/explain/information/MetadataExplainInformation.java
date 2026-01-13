package com.xgen.mongot.index.lucene.explain.information;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.Optional;
import org.bson.BsonDocument;

public record MetadataExplainInformation(
    Optional<String> mongotVersion,
    Optional<String> mongotHostName,
    Optional<String> indexName,
    Optional<BsonDocument> cursorOptions,
    Optional<BsonDocument> optimizationFlags,
    Optional<LuceneMetadataExplainInformation> lucene)
    implements DocumentEncodable {
  static class Fields {
    static final Field.Optional<String> MONGOT_VERSION =
        Field.builder("mongotVersion").stringField().optional().noDefault();

    static final Field.Optional<String> MONGOT_HOST_NAME =
        Field.builder("mongotHostName").stringField().optional().noDefault();

    static final Field.Optional<String> INDEX_NAME =
        Field.builder("indexName").stringField().optional().noDefault();

    static final Field.Optional<BsonDocument> CURSOR_OPTIONS =
        Field.builder("cursorOptions").documentField().optional().noDefault();

    static final Field.Optional<BsonDocument> OPTIMIZATION_FLAGS =
        Field.builder("optimizationFlags").documentField().optional().noDefault();

    static final Field.Optional<LuceneMetadataExplainInformation> LUCENE =
        Field.builder("lucene")
            .classField(LuceneMetadataExplainInformation::fromBson)
            .allowUnknownFields()
            .optional()
            .noDefault();
  }

  public static MetadataExplainInformation fromBson(DocumentParser parser)
      throws BsonParseException {
    return new MetadataExplainInformation(
        parser.getField(Fields.MONGOT_VERSION).unwrap(),
        parser.getField(Fields.MONGOT_HOST_NAME).unwrap(),
        parser.getField(Fields.INDEX_NAME).unwrap(),
        parser.getField(Fields.CURSOR_OPTIONS).unwrap(),
        parser.getField(Fields.OPTIMIZATION_FLAGS).unwrap(),
        parser.getField(Fields.LUCENE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.MONGOT_VERSION, this.mongotVersion)
        .field(Fields.MONGOT_HOST_NAME, this.mongotHostName)
        .field(Fields.INDEX_NAME, this.indexName)
        .field(Fields.CURSOR_OPTIONS, this.cursorOptions)
        .field(Fields.OPTIMIZATION_FLAGS, this.optimizationFlags)
        .field(Fields.LUCENE, this.lucene)
        .build();
  }
}
