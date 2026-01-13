package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.MongoNamespace;
import com.mongodb.internal.client.model.AggregationLevel;
import com.mongodb.internal.operation.AggregateOperation;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.AggregationPipelineBuilder;
import com.xgen.mongot.util.mongodb.ChangeStreamAggregateCommand.FullDocument;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.codecs.Decoder;
import org.bson.conversions.Bson;

public class ChangeStreamAggregateOperationBuilder {

  private final MongoNamespace namespace;
  private final ChangeStreamPipelineBuilder pipelineBuilder;

  private OptionalLong maxTimeMs;
  private OptionalInt batchSize;

  /** $project that filters our fields that are not used for indexing. */
  private Optional<Bson> indexedFieldsProjectionStage;

  /* $project that filters out unused changestream metadata fields. */
  private Optional<Bson> changeStreamMetadataExclusionProjectionStage;

  /**
   * $addFields that adds metadata fields like _id and deleted flag under a separate namespace under
   * indexId field. Required to avoid collision with user-defined fields.
   */
  private Optional<Bson> metadataAddFieldsStage;

  /**
   * Aggregation stages defined in the view. Empty if the index is created on a collection or the
   * view has an empty pipeline.
   */
  private Optional<List<Bson>> viewDefinedStages;

  /** $changeStreamSplitLargeEvent stage for large event splitting. */
  private Optional<Bson> changeStreamSplitLargeEventStage;

  public ChangeStreamAggregateOperationBuilder(MongoNamespace namespace) {
    Check.argNotNull(namespace, "namespace");
    this.maxTimeMs = OptionalLong.empty();
    this.batchSize = OptionalInt.empty();
    this.indexedFieldsProjectionStage = Optional.empty();
    this.changeStreamMetadataExclusionProjectionStage = Optional.empty();
    this.viewDefinedStages = Optional.empty();
    this.metadataAddFieldsStage = Optional.empty();
    this.changeStreamSplitLargeEventStage = Optional.empty();
    this.namespace = namespace;
    this.pipelineBuilder = new ChangeStreamPipelineBuilder();
  }

  public ChangeStreamAggregateOperationBuilder batchSize(int value) {
    this.batchSize = OptionalInt.of(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder maxTimeMs(long value) {
    this.maxTimeMs = OptionalLong.of(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder indexedFieldsProjectionStage(Bson value) {
    this.indexedFieldsProjectionStage = Optional.of(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder changeStreamMetadataExclusionProjectionStage(
      Bson value) {
    this.changeStreamMetadataExclusionProjectionStage = Optional.of(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder viewDefinedStages(List<Bson> viewDefinedStages) {
    this.viewDefinedStages = Optional.of(viewDefinedStages);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder metadataAddFieldsStage(
      BsonDocument metadataAddFieldsStage) {
    this.metadataAddFieldsStage = Optional.of(metadataAddFieldsStage);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder fullDocument(FullDocument value) {
    this.pipelineBuilder.fullDocument(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder startAfter(BsonDocument value) {
    this.pipelineBuilder.startAfter(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder startAtOperationTime(BsonTimestamp value) {
    this.pipelineBuilder.startAtOperationTime(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder showMigrationEvents(Boolean value) {
    this.pipelineBuilder.showMigrationEvents(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder showExpandedEvents(Boolean value) {
    this.pipelineBuilder.showExpandedEvents(value);
    return this;
  }

  public ChangeStreamAggregateOperationBuilder splitLargeEvent(boolean value) {
    if (value) {
      this.changeStreamSplitLargeEventStage =
          Optional.of(new BsonDocument("$changeStreamSplitLargeEvent", new BsonDocument()));
    }
    return this;
  }

  public ChangeStreamAggregateOperationBuilder matchCollectionUuidForUpdateLookup(Boolean value) {
    this.pipelineBuilder.matchCollectionUuidForUpdateLookup(value);
    return this;
  }

  public AggregateOperationTemplate build() {
    return new AggregateOperationTemplate(this);
  }

  private <T> AggregateOperation<T> build(Decoder<T> decoder) {
    Check.argNotNull(decoder, "decoder");

    AggregateOperation<T> aggregate =
        new AggregateOperation<>(
            this.namespace, createChangeStreamPipeline(), decoder, AggregationLevel.COLLECTION);

    this.batchSize.ifPresent(aggregate::batchSize);
    this.maxTimeMs.ifPresent(value -> aggregate.maxTime(value, TimeUnit.MILLISECONDS));

    return aggregate;
  }

  private List<BsonDocument> createChangeStreamPipeline() {
    var changeStreamChange = new BsonDocument("$changeStream", this.pipelineBuilder.build());

    return new AggregationPipelineBuilder()
            .addStage(changeStreamChange)
            .addStage(this.metadataAddFieldsStage)
            .addMultipleStages(this.viewDefinedStages)
            // projection of the fields used in the index appears after view-defined stages
            // as view-defined stages might need access to the full document
            .addStage(this.indexedFieldsProjectionStage)
            .addStage(this.changeStreamMetadataExclusionProjectionStage)
            // changeStreamSplitLargeEvent must be the last stage to minimize data being split
            .addStage(this.changeStreamSplitLargeEventStage)
            .build()
            .stream()
            .map(Bson::toBsonDocument)
            .collect(Collectors.toList());
  }

  private static class ChangeStreamPipelineBuilder {

    private final BsonDocument pipeline = new BsonDocument();

    public void fullDocument(FullDocument value) {
      if (value == FullDocument.UPDATE_LOOKUP) {
        this.pipeline.put("fullDocument", new BsonString("updateLookup"));
      } else {
        this.pipeline.remove("fullDocument");
      }
    }

    public void startAfter(BsonDocument value) {
      this.pipeline.put("startAfter", value);
    }

    public void startAtOperationTime(BsonTimestamp value) {
      this.pipeline.put("startAtOperationTime", value);
    }

    public void showMigrationEvents(Boolean value) {
      this.pipeline.put("showMigrationEvents", new BsonBoolean(value));
    }

    public void showExpandedEvents(Boolean value) {
      this.pipeline.put("showExpandedEvents", new BsonBoolean(value));
    }

    public void matchCollectionUuidForUpdateLookup(Boolean value) {
      // matchCollectionUuidForUpdateLookup is not recognized in older MongoDB versions, so we only
      // append it if it is present. MMS will not pass in matchCollectionUuidForUpdateLookup if the
      // MongoDB version is unsupported.
      if (value) {
        this.pipeline.put("matchCollectionUUIDForUpdateLookup", new BsonBoolean(true));
      }
    }

    public BsonDocument build() {
      return this.pipeline;
    }
  }

  public static class AggregateOperationTemplate {

    private final ChangeStreamAggregateOperationBuilder builder;

    private AggregateOperationTemplate(ChangeStreamAggregateOperationBuilder builder) {
      this.builder = builder;
    }

    public <T> AggregateOperation<T> create(Decoder<T> decoder) {
      return this.builder.build(decoder);
    }
  }
}
