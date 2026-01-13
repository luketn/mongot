package com.xgen.mongot.config.manager.metrics;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

public record GroupedIndexGenerationMetrics(
    ObjectId indexId, List<IndexGenerationStateMetrics> indexGenerationStateMetrics)
    implements DocumentEncodable {
  private static class Fields {
    private static final Field.Required<ObjectId> INDEX_ID =
        Field.builder("indexId").objectIdField().required();

    private static final Field.Required<List<IndexGenerationStateMetrics>> INDEX_GENERATION_STATES =
        Field.builder("generations")
            .classField(IndexGenerationStateMetrics::fromBson)
            .disallowUnknownFields()
            .asList()
            .required();
  }

  @VisibleForTesting
  public GroupedIndexGenerationMetrics {}

  @Override
  public BsonDocument toBson() {
    return BsonDocumentBuilder.builder()
        .field(Fields.INDEX_ID, this.indexId)
        .field(Fields.INDEX_GENERATION_STATES, this.indexGenerationStateMetrics)
        .build();
  }

  public static GroupedIndexGenerationMetrics fromBson(DocumentParser parser)
      throws BsonParseException {
    return new GroupedIndexGenerationMetrics(
        parser.getField(Fields.INDEX_ID).unwrap(),
        parser.getField(Fields.INDEX_GENERATION_STATES).unwrap());
  }
}
