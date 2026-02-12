package com.xgen.mongot.config.provider.community;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.catalogservice.IndexStats;
import com.xgen.mongot.catalogservice.IndexStatsEntry;
import com.xgen.mongot.catalogservice.MetadataService;
import com.xgen.mongot.catalogservice.MetadataServiceException;
import com.xgen.mongot.catalogservice.ServerState;
import com.xgen.mongot.catalogservice.ServerStateEntry;
import com.xgen.mongot.config.manager.CachedIndexInfoProvider;
import com.xgen.mongot.index.AggregatedIndexMetrics;
import com.xgen.mongot.index.IndexDetailedStatus;
import com.xgen.mongot.index.IndexInformation;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinition;
import com.xgen.mongot.index.definition.ViewDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.bson.types.ObjectId;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class CommunityMetadataUpdaterTest {

  private CommunityServerInfo serverInfo;
  private MetadataService metadataService;
  private ServerState serverState;
  private IndexStats indexStats;
  private CachedIndexInfoProvider indexInfoProvider;
  private SimpleMeterRegistry meterRegistry;
  private CommunityMetadataUpdater heartbeater;

  @Before
  public void setUp() {
    this.serverInfo = new CommunityServerInfo(new ObjectId(), Optional.of("test-server"));
    this.metadataService = mock(MetadataService.class);
    this.serverState = mock(ServerState.class);
    this.indexStats = mock(IndexStats.class);
    this.indexInfoProvider = mock(CachedIndexInfoProvider.class);
    this.meterRegistry = new SimpleMeterRegistry();
    when(this.metadataService.getServerState()).thenReturn(this.serverState);
    when(this.metadataService.getIndexStats()).thenReturn(this.indexStats);
    this.heartbeater =
        new CommunityMetadataUpdater(
            this.serverInfo,
            this.metadataService,
            this.indexInfoProvider,
            this.meterRegistry,
            Duration.ofMillis(10));
  }

  @Test
  public void run_upsertsCorrectServerStateEntry() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    this.heartbeater.run();

    // Capture the upserted entry
    ArgumentCaptor<ServerStateEntry> entryCaptor = ArgumentCaptor.forClass(ServerStateEntry.class);
    verify(this.serverState).upsert(entryCaptor.capture());

    // Verify the upserted entry has correct serverId and serverName
    ServerStateEntry capturedEntry = entryCaptor.getValue();
    assertEquals(this.serverInfo.id(), capturedEntry.serverId());
    assertEquals(this.serverInfo.getExternalName(), capturedEntry.serverName());
  }

  @Test
  public void run_handlesMetadataServiceException() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    // Simulate MetadataServiceException on upsert
    doThrow(MetadataServiceException.createTransient(new RuntimeException("test exception")))
        .when(this.serverState)
        .upsert(any(ServerStateEntry.class));

    // Should not throw - exception is caught and logged
    this.heartbeater.run();

    // Verify that upsert was called
    verify(this.serverState).upsert(any(ServerStateEntry.class));
  }

  @Test
  public void run_afterStop_throwsIllegalStateException() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    this.heartbeater.stop();

    // Should throw IllegalStateException because heartbeater is closed
    IllegalStateException exception =
        assertThrows(IllegalStateException.class, () -> this.heartbeater.run());

    assertEquals("cannot call update() after close()", exception.getMessage());
  }

  @Test
  public void run_firstPass_createsIndexes() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    // First run should create indexes
    this.heartbeater.run();

    // Verify createCollectionIndexes was called
    verify(this.indexStats).createCollectionIndexes();

    // Verify server state and index stats were updated
    verify(this.serverState).upsert(any(ServerStateEntry.class));
    verify(this.indexInfoProvider).refreshIndexInfos();
    verify(this.indexInfoProvider).getIndexInfos();
  }

  @Test
  public void run_firstPass_indexCreationFails_doesNotUpdateStats() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    // Simulate index creation failure
    doThrow(MetadataServiceException.createTransient(new RuntimeException("DB not ready")))
        .when(this.indexStats)
        .createCollectionIndexes();

    // First run should fail to create indexes
    this.heartbeater.run();

    // Verify createCollectionIndexes was called
    verify(this.indexStats).createCollectionIndexes();

    // Verify server state and index stats were NOT updated
    verify(this.serverState, never()).upsert(any(ServerStateEntry.class));
    verify(this.indexInfoProvider, never()).refreshIndexInfos();
    verify(this.indexInfoProvider, never()).getIndexInfos();
  }

  @Test
  public void run_secondPass_doesNotRecreateIndexes() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    // First run creates indexes
    this.heartbeater.run();

    // Second run should not try to create indexes again
    this.heartbeater.run();

    // Verify createCollectionIndexes was only called once
    verify(this.indexStats, times(1)).createCollectionIndexes();

    // Verify server state and index stats were updated twice
    verify(this.serverState, times(2)).upsert(any(ServerStateEntry.class));
    verify(this.indexInfoProvider, times(2)).refreshIndexInfos();
  }

  @Test
  public void run_updatesIndexStats_withEmptyList() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    // Mock empty index list
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    this.heartbeater.run();

    // Verify upsertAll was called with empty list
    verify(this.indexStats).upsertAll(Collections.emptySet());
  }

  @Test
  public void run_updatesIndexStats_withSearchIndex() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition searchDef = createSearchIndexDefinition(indexId);
    IndexInformation indexInfo =
        createSearchIndexInformation(searchDef, new IndexStatus(IndexStatus.StatusCode.STEADY));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));

    this.heartbeater.run();

    // Capture the upserted entries
    ArgumentCaptor<Set<IndexStatsEntry>> entriesCaptor = ArgumentCaptor.forClass(Set.class);
    verify(this.indexStats).upsertAll(entriesCaptor.capture());

    // Verify the upserted entry has correct properties
    Set<IndexStatsEntry> capturedEntries = entriesCaptor.getValue();
    assertEquals(1, capturedEntries.size());
    IndexStatsEntry entry = capturedEntries.stream().findFirst().orElseThrow();
    assertEquals(this.serverInfo.id(), entry.key().serverId());
    assertEquals(indexId, entry.key().indexId());
    assertEquals(IndexDefinition.Type.SEARCH, entry.type());
  }

  @Test
  public void run_updatesIndexStats_withVectorIndex() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    ObjectId indexId = new ObjectId();
    VectorIndexDefinition vectorDef = createVectorIndexDefinition(indexId);
    IndexInformation indexInfo =
        createVectorIndexInformation(vectorDef, new IndexStatus(IndexStatus.StatusCode.STEADY));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));

    this.heartbeater.run();

    // Capture the upserted entries
    ArgumentCaptor<Set<IndexStatsEntry>> entriesCaptor = ArgumentCaptor.forClass(Set.class);
    verify(this.indexStats).upsertAll(entriesCaptor.capture());

    // Verify the upserted entry has correct properties
    Set<IndexStatsEntry> capturedEntries = entriesCaptor.getValue();
    assertEquals(1, capturedEntries.size());
    IndexStatsEntry entry = capturedEntries.stream().findFirst().orElseThrow();
    assertEquals(this.serverInfo.id(), entry.key().serverId());
    assertEquals(indexId, entry.key().indexId());
    assertEquals(IndexDefinition.Type.VECTOR_SEARCH, entry.type());
  }

  @Test
  public void run_updatesIndexStats_withMultipleIndexes() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    ObjectId searchIndexId = new ObjectId();
    ObjectId vectorIndexId = new ObjectId();

    SearchIndexDefinition searchDef = createSearchIndexDefinition(searchIndexId);
    VectorIndexDefinition vectorDef = createVectorIndexDefinition(vectorIndexId);

    IndexInformation searchInfo =
        createSearchIndexInformation(searchDef, new IndexStatus(IndexStatus.StatusCode.STEADY));
    IndexInformation vectorInfo =
        createVectorIndexInformation(vectorDef, new IndexStatus(IndexStatus.StatusCode.STEADY));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(searchInfo, vectorInfo));

    this.heartbeater.run();

    // Capture the upserted entries
    ArgumentCaptor<Set<IndexStatsEntry>> entriesCaptor = ArgumentCaptor.forClass(Set.class);
    verify(this.indexStats).upsertAll(entriesCaptor.capture());

    // Verify both entries were upserted
    Set<IndexStatsEntry> capturedEntries = entriesCaptor.getValue();
    assertEquals(2, capturedEntries.size());
  }

  @Test
  public void run_handlesIndexStatsUpdateException() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    ObjectId indexId = new ObjectId();
    SearchIndexDefinition searchDef = createSearchIndexDefinition(indexId);
    IndexInformation indexInfo =
        createSearchIndexInformation(searchDef, new IndexStatus(IndexStatus.StatusCode.STEADY));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo));

    // Simulate exception during upsertAll
    doThrow(MetadataServiceException.createTransient(new RuntimeException("test exception")))
        .when(this.indexStats)
        .upsertAll(anySet());

    // Should not throw - exception is caught and logged
    this.heartbeater.run();

    // Verify that upsertAll was called
    verify(this.indexStats).upsertAll(anySet());
  }

  @Test
  public void run_firstRun_loadsIndexStatsCacheFromMetadata() throws Exception {
    // Create some existing index stats entries in metadata
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();

    SearchIndexDefinition searchDef1 = createSearchIndexDefinition(indexId1);
    SearchIndexDefinition searchDef2 = createSearchIndexDefinition(indexId2);

    IndexStatsEntry entry1 =
        createSearchIndexStatsEntry(
            indexId1, searchDef1, new IndexStatus(IndexStatus.StatusCode.STEADY));

    IndexStatsEntry entry2 =
        createSearchIndexStatsEntry(
            indexId2, searchDef2, new IndexStatus(IndexStatus.StatusCode.STEADY));

    // Mock the list method to return existing entries
    when(this.indexStats.list(any())).thenReturn(List.of(entry1, entry2));

    // Mock empty current indexes
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    // First run should load the cache
    this.heartbeater.run();

    // Verify list was called with the correct server ID filter
    verify(this.indexStats).list(IndexStatsEntry.serverIdFilter(this.serverInfo.id()));

    // Capture the deleted keys
    ArgumentCaptor<Set<IndexStatsEntry.IndexStatsKey>> keysCaptor =
        ArgumentCaptor.forClass(Set.class);
    verify(this.indexStats).deleteAll(keysCaptor.capture());

    // Since the cache was loaded with 2 entries but current indexes are empty,
    // both entries should be deleted
    Set<IndexStatsEntry.IndexStatsKey> capturedKeys = keysCaptor.getValue();
    assertEquals(2, capturedKeys.size());
    assertTrue(capturedKeys.contains(entry1.key()));
    assertTrue(capturedKeys.contains(entry2.key()));
  }

  @Test
  public void run_firstRun_throwsOnDuplicateIndexStatsKeys() throws Exception {
    // Create two index stats entries with the SAME key (same serverId and indexId)
    ObjectId duplicateIndexId = new ObjectId();

    SearchIndexDefinition searchDef1 = createSearchIndexDefinition(duplicateIndexId);
    SearchIndexDefinition searchDef2 = createSearchIndexDefinition(duplicateIndexId);

    // Both entries have the same key but different definitions
    IndexStatsEntry entry1 =
        createSearchIndexStatsEntry(
            duplicateIndexId, searchDef1, new IndexStatus(IndexStatus.StatusCode.STEADY));

    IndexStatsEntry entry2 =
        createSearchIndexStatsEntry(
            duplicateIndexId, searchDef2, new IndexStatus(IndexStatus.StatusCode.INITIAL_SYNC));

    // Mock the list method to return entries with duplicate keys
    when(this.indexStats.list(any())).thenReturn(List.of(entry1, entry2));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    // First run should throw IllegalArgumentException when loading the cache
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> this.heartbeater.run());

    assertEquals("duplicate index keys in indexStats collection", exception.getMessage());

    // Verify list was called
    verify(this.indexStats).list(IndexStatsEntry.serverIdFilter(this.serverInfo.id()));
  }

  @Test
  public void run_deletesUnusedIndexes() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    // First run to initialize
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());
    this.heartbeater.run();

    // Create index information for two indexes
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();
    ObjectId indexId3 = new ObjectId();

    SearchIndexDefinition searchDef1 = createSearchIndexDefinition(indexId1);
    SearchIndexDefinition searchDef2 = createSearchIndexDefinition(indexId2);
    SearchIndexDefinition searchDef3 = createSearchIndexDefinition(indexId3);

    IndexInformation indexInfo1 =
        createSearchIndexInformation(searchDef1, new IndexStatus(IndexStatus.StatusCode.STEADY));
    IndexInformation indexInfo2 =
        createSearchIndexInformation(searchDef2, new IndexStatus(IndexStatus.StatusCode.STEADY));
    IndexInformation indexInfo3 =
        createSearchIndexInformation(searchDef3, new IndexStatus(IndexStatus.StatusCode.STEADY));

    // Second run with three indexes
    when(this.indexInfoProvider.getIndexInfos())
        .thenReturn(List.of(indexInfo1, indexInfo2, indexInfo3));
    this.heartbeater.run();

    // Third run with only two indexes (indexId2 was removed)
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1, indexInfo3));
    this.heartbeater.run();

    // Capture all the deleted keys calls
    ArgumentCaptor<Set<IndexStatsEntry.IndexStatsKey>> keysCaptor =
        ArgumentCaptor.forClass(Set.class);
    verify(this.indexStats, times(3)).deleteAll(keysCaptor.capture());

    // Verify that indexId2 was deleted (check the last call)
    List<Set<IndexStatsEntry.IndexStatsKey>> allCapturedKeys = keysCaptor.getAllValues();
    Set<IndexStatsEntry.IndexStatsKey> lastDeletedKeys = allCapturedKeys.get(2);
    assertEquals(1, lastDeletedKeys.size());
    IndexStatsEntry.IndexStatsKey deletedKey = lastDeletedKeys.stream().findFirst().orElseThrow();
    assertEquals(this.serverInfo.id(), deletedKey.serverId());
    assertEquals(indexId2, deletedKey.indexId());
  }

  @Test
  public void run_onlyUpsertsChangedIndexes() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    // First run to initialize
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());
    this.heartbeater.run();

    // Create index information
    ObjectId indexId1 = new ObjectId();
    ObjectId indexId2 = new ObjectId();

    SearchIndexDefinition searchDef1 = createSearchIndexDefinition(indexId1);
    SearchIndexDefinition searchDef2 = createSearchIndexDefinition(indexId2);

    IndexInformation indexInfo1 =
        createSearchIndexInformation(searchDef1, new IndexStatus(IndexStatus.StatusCode.STEADY));
    IndexInformation indexInfo2 =
        createSearchIndexInformation(searchDef2, new IndexStatus(IndexStatus.StatusCode.STEADY));

    // Second run with two indexes
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1, indexInfo2));
    this.heartbeater.run();

    // Capture the upserted entries from the second run
    ArgumentCaptor<Set<IndexStatsEntry>> entriesCaptor1 = ArgumentCaptor.forClass(Set.class);
    verify(this.indexStats, times(2)).upsertAll(entriesCaptor1.capture());

    // Verify both indexes were upserted (called once with both indexes)
    List<Set<IndexStatsEntry>> allCapturedEntries1 = entriesCaptor1.getAllValues();
    Set<IndexStatsEntry> secondRunEntries = allCapturedEntries1.get(1);
    assertEquals(2, secondRunEntries.size());
    assertTrue(secondRunEntries.stream().anyMatch(e -> e.key().indexId().equals(indexId1)));
    assertTrue(secondRunEntries.stream().anyMatch(e -> e.key().indexId().equals(indexId2)));

    // Third run with same indexes (no changes)
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1, indexInfo2));
    this.heartbeater.run();

    // Verify upsertAll was called with empty set (no changes) - this is the 2nd time overall
    // (first time was on the initial run with empty indexes)
    verify(this.indexStats, times(2)).upsertAll(Collections.emptySet());

    // Fourth run with one index changed (status changed to INITIAL_SYNC)
    IndexInformation indexInfo1Changed =
        createSearchIndexInformation(
            searchDef1, new IndexStatus(IndexStatus.StatusCode.INITIAL_SYNC));

    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1Changed, indexInfo2));
    this.heartbeater.run();

    // Capture the upserted entries from the fourth run
    ArgumentCaptor<Set<IndexStatsEntry>> entriesCaptor2 = ArgumentCaptor.forClass(Set.class);
    verify(this.indexStats, times(4)).upsertAll(entriesCaptor2.capture());

    // Verify only the changed index was upserted
    List<Set<IndexStatsEntry>> allCapturedEntries2 = entriesCaptor2.getAllValues();
    Set<IndexStatsEntry> fourthRunEntries = allCapturedEntries2.get(3);
    assertEquals(1, fourthRunEntries.size());
    IndexStatsEntry changedEntry = fourthRunEntries.stream().findFirst().orElseThrow();
    assertEquals(indexId1, changedEntry.key().indexId());
  }

  @Test
  public void run_throwsOnDuplicateIndexStatsFromIndexInfoProvider() throws Exception {
    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());

    // First run to initialize
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());
    this.heartbeater.run();

    // Create two IndexInformation objects with the SAME indexId
    // This simulates a bug where the indexInfoProvider returns duplicate indexes
    ObjectId duplicateIndexId = new ObjectId();

    SearchIndexDefinition searchDef1 = createSearchIndexDefinition(duplicateIndexId);
    SearchIndexDefinition searchDef2 = createSearchIndexDefinition(duplicateIndexId);

    IndexInformation indexInfo1 =
        createSearchIndexInformation(searchDef1, new IndexStatus(IndexStatus.StatusCode.STEADY));
    IndexInformation indexInfo2 =
        createSearchIndexInformation(
            searchDef2, new IndexStatus(IndexStatus.StatusCode.INITIAL_SYNC));

    // Second run with duplicate index IDs should throw IllegalArgumentException
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(List.of(indexInfo1, indexInfo2));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> this.heartbeater.run());

    assertEquals("Duplicate index stats entries for a given key", exception.getMessage());

    // Verify refreshIndexInfos was called
    verify(this.indexInfoProvider, times(2)).refreshIndexInfos();
  }

  private SearchIndexDefinition createSearchIndexDefinition(ObjectId indexId) {
    return SearchIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name("test-search-index")
        .database("test-db")
        .lastObservedCollectionName("test-collection")
        .collectionUuid(UUID.randomUUID())
        .view(ViewDefinition.existing("test-view", List.of()))
        .numPartitions(IndexDefinition.Fields.NUM_PARTITIONS.getDefaultValue())
        .synonyms(
            SynonymMappingDefinitionBuilder.builder()
                .name("test-synonym")
                .synonymSourceDefinition("test-coll")
                .analyzer("lucene.english")
                .buildAsList())
        .dynamicMapping()
        .definitionVersionCreatedAt(Instant.now())
        .definitionVersion(0L)
        .build();
  }

  private VectorIndexDefinition createVectorIndexDefinition(ObjectId indexId) {
    return VectorIndexDefinitionBuilder.builder()
        .indexId(indexId)
        .name("test-vector-index")
        .database("test-db")
        .lastObservedCollectionName("test-collection")
        .collectionUuid(UUID.randomUUID())
        .numPartitions(1)
        .withCosineVectorField("test.vector.field", 128)
        .withFilterPath("test.filter.field")
        .withDefinitionVersionCreatedAt(Optional.of(Instant.now()))
        .withDefinitionVersion(Optional.of(0L))
        .build();
  }

  private IndexInformation.Search createSearchIndexInformation(
      SearchIndexDefinition definition, IndexStatus status) {
    IndexDetailedStatus.Search detailedStatus =
        new IndexDetailedStatus.Search(
            Collections.emptyMap(), definition, status, Optional.empty());

    return new IndexInformation.Search(
        definition,
        status,
        Collections.emptyList(),
        new AggregatedIndexMetrics(0L, 0L, new org.bson.BsonTimestamp(0L), 0L),
        Optional.of(detailedStatus),
        Optional.empty(),
        Collections.emptyMap());
  }

  private IndexInformation.Vector createVectorIndexInformation(
      VectorIndexDefinition definition, IndexStatus status) {
    IndexDetailedStatus.Vector detailedStatus =
        new IndexDetailedStatus.Vector(definition, status, Optional.empty());

    return new IndexInformation.Vector(
        definition,
        status,
        Collections.emptyList(),
        new AggregatedIndexMetrics(0L, 0L, new org.bson.BsonTimestamp(0L), 0L),
        Optional.of(detailedStatus),
        Optional.empty());
  }

  private IndexStatsEntry createSearchIndexStatsEntry(
      ObjectId indexId, SearchIndexDefinition definition, IndexStatus status) {
    return new IndexStatsEntry(
        new IndexStatsEntry.IndexStatsKey(this.serverInfo.id(), indexId),
        IndexDefinition.Type.SEARCH,
        Optional.of(
            new IndexStatsEntry.DetailedIndexStats(
                status, definition, Optional.of(Collections.emptyMap()))),
        Optional.empty());
  }

  @Test
  public void stop_canBeCalledDuringFixedDelay_evenWithSlowRun() throws Exception {
    // This test verifies that stop() can be called successfully even when run() takes a long time
    // to execute. This is important because:
    // 1. scheduleWithFixedDelay() waits for run() to complete before starting the delay
    // 2. If someone changes to scheduleAtFixedRate(), there would be no gap between executions
    //    when run() is slow, potentially causing stop() to block indefinitely
    // 3. This test will FAIL if the implementation is changed to scheduleAtFixedRate()

    when(this.indexStats.list(any())).thenReturn(Collections.emptyList());
    when(this.indexInfoProvider.getIndexInfos()).thenReturn(Collections.emptyList());

    // Use a period that's shorter than run() execution time
    // With fixedDelay: creates a gap after slow run() completes
    // With fixedRate: no gap if run() takes longer than the period
    CommunityMetadataUpdater updater =
        new CommunityMetadataUpdater(
            this.serverInfo,
            this.metadataService,
            this.indexInfoProvider,
            this.meterRegistry,
            Duration.ofMillis(500)); // 500ms period

    // Track when run() starts and completes
    CountDownLatch firstRunStartedLatch = new CountDownLatch(1);
    CountDownLatch firstRunCompletedLatch = new CountDownLatch(1);
    CountDownLatch secondRunStartedLatch = new CountDownLatch(1);
    AtomicBoolean allowSecondRunToComplete = new AtomicBoolean(false);

    // Make run() take longer than the period to simulate high latency
    // This ensures that with fixedRate, the second run would start immediately
    doAnswer(
            invocation -> {
              // First run
              if (firstRunStartedLatch.getCount() > 0) {
                firstRunStartedLatch.countDown();
                // Block for 1 second (longer than the 500ms period)
                Thread.sleep(1000);
                firstRunCompletedLatch.countDown();
              } else {
                // Second run - should only happen with fixedRate
                secondRunStartedLatch.countDown();
                // Block until we're ready to let second run complete
                while (!allowSecondRunToComplete.get()) {
                  Thread.sleep(10);
                }
              }
              return null;
            })
        .when(this.indexInfoProvider)
        .refreshIndexInfos();

    // Start the updater
    updater.start();

    // Wait for the first run() to start
    assertTrue(
        "First run should start within 2 seconds", firstRunStartedLatch.await(2, TimeUnit.SECONDS));

    // Wait for first run() to complete (it sleeps for 1000ms)
    assertTrue(
        "First run should complete within 3 seconds",
        firstRunCompletedLatch.await(3, TimeUnit.SECONDS));

    // At this point with scheduleWithFixedDelay:
    // - First run() has completed after 1000ms
    // - The executor is now in the 500ms delay period before the next run()
    // - There is a window where stop() can acquire the monitor
    //
    // With scheduleAtFixedRate:
    // - First run() completed after 1000ms (period was 500ms)
    // - The second run() would have been scheduled to start at t=500ms
    // - Since first run didn't complete until t=1000ms, second run starts immediately
    // - There would be no gap, and stop() would have to wait for the second run() to complete

    // Call stop() immediately - don't wait, to maximize chance of catching it in the delay period
    // With fixedDelay: there's a 500ms window where stop() can succeed
    // With fixedRate: second run starts immediately, so stop() will block

    // Call stop() from a different thread
    CountDownLatch stopCompletedLatch = new CountDownLatch(1);
    AtomicBoolean stopCompleted = new AtomicBoolean(false);

    Thread stopThread =
        new Thread(
            () -> {
              updater.stop();
              stopCompleted.set(true);
              stopCompletedLatch.countDown();
            });
    stopThread.start();

    // Verify that stop() completes quickly during the fixed delay period
    // With fixedDelay: stop() should complete quickly (within 2 seconds) because we're in the delay
    // With fixedRate: stop() would block because second run() is executing and holding the monitor
    // We use a generous 2-second timeout to account for:
    // - Thread scheduling delays on slower/busy machines
    // - GC pauses
    // - Other system load
    boolean stopCompletedInTime = stopCompletedLatch.await(2, TimeUnit.SECONDS);

    // Clean up: allow second run to complete if it started
    allowSecondRunToComplete.set(true);

    assertTrue(
        "Stop should complete within 2 seconds when called during fixed delay. "
            + "If this fails, the implementation may have been changed to scheduleAtFixedRate() "
            + "or the delay period is too short relative to run() execution time. "
            + "Second run started: "
            + (secondRunStartedLatch.getCount() == 0),
        stopCompletedInTime);
    assertTrue("Stop should have completed", stopCompleted.get());

    // With fixedDelay, second run should NOT have started because stop() cancelled it
    assertEquals(
        "Second run should not have started with scheduleWithFixedDelay",
        1,
        secondRunStartedLatch.getCount());

    // Verify the executor was shut down properly
    verify(this.indexStats, times(1)).createCollectionIndexes();
    verify(this.serverState, times(1)).upsert(any(ServerStateEntry.class));
  }
}
