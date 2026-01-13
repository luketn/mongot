package com.xgen.testing.mongot.integration.index.serialization;

import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public class MqlFilterTestResult implements DocumentEncodable {

  static class Fields {
    static final Field.Required<MqlFilterTestPipelineResult> VECTOR_SEARCH_RESULT =
        Field.builder("$vectorSearchResult")
            .classField(MqlFilterTestPipelineResult::fromBson)
            .disallowUnknownFields()
            .required();

    static final Field.Required<MqlFilterTestPipelineResult> MATCH_RESULT =
        Field.builder("$matchResult")
            .classField(MqlFilterTestPipelineResult::fromBson)
            .disallowUnknownFields()
            .required();
  }

  private final MqlFilterTestPipelineResult vectorSearchResult;
  private final MqlFilterTestPipelineResult matchResult;

  MqlFilterTestResult(
      MqlFilterTestPipelineResult vectorSearchResult, MqlFilterTestPipelineResult matchResult) {
    this.vectorSearchResult = vectorSearchResult;
    this.matchResult = matchResult;
  }

  public static MqlFilterTestResult fromBson(DocumentParser parser) throws BsonParseException {
    return new MqlFilterTestResult(
        parser.getField(Fields.VECTOR_SEARCH_RESULT).unwrap(),
        parser.getField(Fields.MATCH_RESULT).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    BsonDocumentBuilder builder = BsonDocumentBuilder.builder();
    return builder
        .field(Fields.VECTOR_SEARCH_RESULT, this.vectorSearchResult)
        .field(Fields.MATCH_RESULT, this.matchResult)
        .build();
  }

  public MqlFilterTestPipelineResult getVectorSearchResult() {
    return this.vectorSearchResult;
  }

  public MqlFilterTestPipelineResult getMatchResult() {
    return this.matchResult;
  }
}
