package com.xgen.mongot.config.manager.metrics;

import com.xgen.mongot.index.IndexGenerationMetrics;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.util.bson.parser.BsonDocumentBuilder;
import com.xgen.mongot.util.bson.parser.BsonParseException;
import com.xgen.mongot.util.bson.parser.DocumentEncodable;
import com.xgen.mongot.util.bson.parser.DocumentParser;
import com.xgen.mongot.util.bson.parser.Field;
import org.bson.BsonDocument;

public record IndexGenerationStateMetrics(
    IndexGenerationMetrics indexGenerationMetrics, IndexConfigState state)
    implements DocumentEncodable {
  private static class Fields {
    private static final Field.Required<IndexConfigState> GENERATION_STATE =
        Field.builder("generationState").enumField(IndexConfigState.class).asCamelCase().required();
  }

  public static IndexGenerationStateMetrics live(InitializedIndex initializedIndex) {
    return new IndexGenerationStateMetrics(
        new IndexGenerationMetrics(
            initializedIndex.getGenerationId(), initializedIndex.getMetrics()),
        IndexConfigState.LIVE);
  }

  public static IndexGenerationStateMetrics staged(InitializedIndex initializedIndex) {
    return new IndexGenerationStateMetrics(
        new IndexGenerationMetrics(
            initializedIndex.getGenerationId(), initializedIndex.getMetrics()),
        IndexConfigState.STAGED);
  }

  public static IndexGenerationStateMetrics phasingOut(InitializedIndex initializedIndex) {
    return new IndexGenerationStateMetrics(
        new IndexGenerationMetrics(
            initializedIndex.getGenerationId(), initializedIndex.getMetrics()),
        IndexConfigState.PHASING_OUT);
  }

  public static IndexGenerationStateMetrics fromBson(DocumentParser parser)
      throws BsonParseException {
    return new IndexGenerationStateMetrics(
        IndexGenerationMetrics.fromBson(parser), parser.getField(Fields.GENERATION_STATE).unwrap());
  }

  @Override
  public BsonDocument toBson() {
    BsonDocument metrics =
        BsonDocumentBuilder.builder().field(Fields.GENERATION_STATE, this.state).build();
    metrics.putAll(this.indexGenerationMetrics.toBson());

    return metrics;
  }
}
