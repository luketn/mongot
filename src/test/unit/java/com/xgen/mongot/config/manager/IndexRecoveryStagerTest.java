package com.xgen.mongot.config.manager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Var;
import com.mongodb.MongoNamespace;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfo;
import com.xgen.mongot.util.mongodb.serialization.MongoDbCollectionInfos;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonTimestamp;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.FromDataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

@RunWith(Theories.class)
public class IndexRecoveryStagerTest {

  /** All IndexStatuses except RECOVERING. */
  @DataPoints("nonRecoveringIndexStatuses")
  public static Set<IndexStatus> nonRecoveringIndex() {
    return Stream.of(IndexStatus.StatusCode.values())
        .filter(statusCode -> statusCode != IndexStatus.StatusCode.RECOVERING_NON_TRANSIENT)
        .map(IndexStatus::new)
        .collect(Collectors.toSet());
  }

  /** Test that all index statuses besides recovering lead to no changes. */
  @Theory
  public void testNoRecoveringIndexesDoesNothing(
      @FromDataPoints("nonRecoveringIndexStatuses") IndexStatus status) throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(status);
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertLiveIndexesAre(index);
    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testDoesNotExistIndexWithCollectionNotFoundStagesCorrectly() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.doesNotExist(IndexStatus.Reason.COLLECTION_NOT_FOUND));
    mocks.clearInvocations();

    UUID mockCollectionUuid = index.getDefinition().getCollectionUuid();
    String mockDatabase = index.getDefinition().getDatabase();
    String mockCollectionName = index.getDefinition().getLastObservedCollectionName();

    // Scenario 1: Collection doesn't exist on mongod
    stageIndexes(mocks, Set.of());

    // Verify that no index is staged
    @Var var stagedIndex = mocks.staged.getIndex(index.getDefinition().getIndexId());
    assertTrue(stagedIndex.isEmpty());
    mocks.assertNoIndexActivity();

    // Scenario 2: Collection exists on mongod
    var correctCollectionInfos =
        new MongoDbCollectionInfos(
            ImmutableMap.of(
                new MongoNamespace(mockDatabase, mockCollectionName),
                new MongoDbCollectionInfo.Collection(
                    mockCollectionName,
                    new MongoDbCollectionInfo.Collection.Info(mockCollectionUuid))));

    stageIndexes(mocks, correctCollectionInfos.getAllCollectionUuids());

    // Verify the index is staged
    stagedIndex = mocks.staged.getIndex(index.getDefinition().getIndexId());
    assertTrue(stagedIndex.isPresent());
    assertEquals(
        index.getDefinitionGeneration().generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
    mocks.clearInvocations();
  }

  @Test
  public void testRecoveringIndexStagedWhenNoOtherStaged() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.recoveringNonTransient(new BsonTimestamp()));
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertOneIndexCreated();
    mocks.assertAtLeastOneIndexStagedAndReplicated();
    mocks.assertJournalPersistedAtLeastOnce();

    var stagedIndex = mocks.staged.getIndex(index.getDefinition().getIndexId());
    assertTrue(stagedIndex.isPresent());
    assertEquals(
        index.getDefinitionGeneration().generation().nextAttempt(),
        stagedIndex.get().getGenerationId().generation);
  }

  @Test
  public void testIndexNotStagedIfAlreadyStagedUser() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.steady());

    var stagedIndex = mocks.stageIndex(index.getDefinition().getIndexId());
    stagedIndex.getIndex().setStatus(IndexStatus.initialSync());
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertNoIndexActivity();
  }

  @Test
  public void testIndexNotStagedIfAlreadyStagedAttempt() throws Exception {
    var mocks = getEmptyMocks();
    var index = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    index.getIndex().setStatus(IndexStatus.steady());

    var stagedIndex = mocks.stageIndexAttempt(index.getDefinition().getIndexId());
    stagedIndex.getIndex().setStatus(IndexStatus.initialSync());
    mocks.clearInvocations();

    stageIndexes(mocks, Set.of());

    mocks.assertNoIndexActivity();
  }

  private void stageIndexes(ConfigStateMocks mocks, Set<UUID> directMongodCollectionSet)
      throws Exception {
    var featureFlags =
        FeatureFlags.withDefaults()
            .enable(Feature.SHUT_DOWN_REPLICATION_WHEN_COLLECTION_NOT_FOUND)
            .build();
    IndexRecoveryStager.stageRecoveryAttempts(
        mocks.configState, directMongodCollectionSet, featureFlags);
  }

  private ConfigStateMocks getEmptyMocks() throws Exception {
    return ConfigStateMocks.create();
  }
}
