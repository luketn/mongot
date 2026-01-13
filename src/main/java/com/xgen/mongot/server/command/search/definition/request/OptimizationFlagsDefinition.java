package com.xgen.mongot.server.command.search.definition.request;

import com.xgen.mongot.index.query.QueryOptimizationFlags;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record OptimizationFlagsDefinition(boolean omitSearchDocumentResults)
    implements DocumentEncodable {
  private static class Fields {
    static final Field.WithDefault<Boolean> OMIT_SEARCH_DOCUMENT_RESULTS =
        Field.builder("omitSearchDocumentResults").booleanField().optional().withDefault(false);
  }

  public static OptimizationFlagsDefinition fromBson(DocumentParser parser)
      throws BsonParseException {
    return new OptimizationFlagsDefinition(
        parser.getField(Fields.OMIT_SEARCH_DOCUMENT_RESULTS).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.OMIT_SEARCH_DOCUMENT_RESULTS, this.omitSearchDocumentResults)
        .build();
  }

  public QueryOptimizationFlags toQueryOptimizationFlags() {
    return new QueryOptimizationFlags(this.omitSearchDocumentResults);
  }
}
