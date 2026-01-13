package com.xgen.mongot.replication.mongodb.common;

import static com.google.common.truth.Truth.assertThat;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.mongodb.MongoNamespace;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.bson.codecs.Decoder;
import org.junit.Test;

public class ChangeStreamAggregateOperationFactoryTest {
  @Test
  public void testChangeStreamMetadataExclusionProjectionStage() {
    String databaseName = "testDatabase";
    String collectionName = "testChangeStreamMetadataExclusionProjectionStage";

    // Create a command factory with excluded fields and check the generated pipeline
    List<String> excludedFields = List.of("excluded1", "excluded2", "nested.field");
    ChangeStreamAggregateOperationFactory factory =
        new ChangeStreamAggregateOperationFactory(
            MOCK_INDEX_DEFINITION,
            0, // maxTimeMs
            excludedFields,
            false,
            false);
    List<BsonDocument> pipeline =
        factory
            .fromResumeInfo(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace(databaseName, collectionName), new BsonDocument()),
                ChangeStreamModeSelector.ChangeStreamMode.getDefault())
            .create((Decoder<RawBsonDocument>) mock(Decoder.class))
            .getPipeline();
    assertThat(pipeline).isNotEmpty();

    // The exclusion projection stage should be the last one
    BsonDocument exclusionStage = pipeline.getLast();
    assertTrue("Expected last stage to be $project", exclusionStage.containsKey("$project"));
    BsonDocument projectFields = exclusionStage.getDocument("$project");
    excludedFields.forEach(
        field -> assertTrue(field + " should be excluded", projectFields.containsKey(field)));
  }

  @Test
  public void testMatchCollectionUuidForUpdateLookup() {
    String databaseName = "testDatabase";
    String collectionName = "testMatchCollectionUuidForUpdateLookup";

    // Create a command factory with matchCollectionUuidForUpdateLookup and check the generated
    // pipeline
    ChangeStreamAggregateOperationFactory factory =
        new ChangeStreamAggregateOperationFactory(
            MOCK_INDEX_DEFINITION,
            0, // maxTimeMs
            List.of(),
            true,
            false);
    List<BsonDocument> pipeline =
        factory
            .fromResumeInfo(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace(databaseName, collectionName), new BsonDocument()),
                ChangeStreamModeSelector.ChangeStreamMode.getDefault())
            .create((Decoder<RawBsonDocument>) mock(Decoder.class))
            .getPipeline();
    assertThat(pipeline).isNotEmpty();
    assertThat(
            pipeline
                .getFirst()
                .get("$changeStream")
                .asDocument()
                .get("matchCollectionUUIDForUpdateLookup")
                .asBoolean()
                .getValue())
        .isTrue();
  }

  @Test
  public void testChangeStreamSplitLargeEventStage() {
    String databaseName = "testDatabase";
    String collectionName = "testChangeStreamSplitLargeEventStage";

    ChangeStreamAggregateOperationFactory factory =
        new ChangeStreamAggregateOperationFactory(
            MOCK_INDEX_DEFINITION,
            0, // maxTimeMs
            List.of(),
            false,
            true);
    List<BsonDocument> pipeline =
        factory
            .fromResumeInfo(
                ChangeStreamResumeInfo.create(
                    new MongoNamespace(databaseName, collectionName), new BsonDocument()),
                ChangeStreamModeSelector.ChangeStreamMode.getDefault())
            .create((Decoder<RawBsonDocument>) mock(Decoder.class))
            .getPipeline();
    assertThat(pipeline).isNotEmpty();
    BsonDocument splitLargeEventStage = pipeline.getLast();
    assertTrue(
        "Expected last stage to be $changeStreamSplitLargeEvent",
        splitLargeEventStage.containsKey("$changeStreamSplitLargeEvent"));
  }
}
