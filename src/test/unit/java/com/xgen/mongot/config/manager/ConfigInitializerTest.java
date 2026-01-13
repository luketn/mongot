package com.xgen.mongot.config.manager;

import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_DEFINITION_GENERATION;
import static com.xgen.testing.mongot.mock.index.SearchIndex.MOCK_INDEX_ID;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_DEFINITION;
import static com.xgen.testing.mongot.mock.index.VectorIndex.MOCK_VECTOR_INDEX_DEFINITION_GENERATION;
import static org.mockito.Mockito.verify;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.config.util.Invariants;
import com.xgen.mongot.index.Index;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.VectorIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import com.xgen.testing.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionBuilder;
import com.xgen.testing.mongot.index.definition.SearchIndexDefinitionGenerationBuilder;
import com.xgen.testing.mongot.index.definition.VectorIndexDefinitionBuilder;
import com.xgen.testing.mongot.mock.index.VectorIndex;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class ConfigInitializerTest {
  @Test
  public void testInitWithEmptyJournalDoesntModifyState() throws Exception {
    var emptyConfig = ConfigJournalV1Builder.builder().build();

    ConfigStateMocks mocks = initializeWithConfig(emptyConfig);

    mocks.assertNoIndexActivity();
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEmpty();
  }

  @Test
  public void testOneSearchIndexToDropIsDropped() throws Exception {
    var config =
        ConfigJournalV1Builder.builder().deletedIndex(MOCK_INDEX_DEFINITION_GENERATION).build();

    ConfigStateMocks mocks = initializeWithConfig(config);

    // Shouldn't have tried to create any indexes.
    mocks.assertNoIndexCatalogedAndReplicated();
    mocks.assertOneIndexCreated();
    mocks.assertIndexCreated(MOCK_INDEX_DEFINITION);

    // Make sure that the index was actually dropped
    Assert.assertEquals(1, mocks.createdIndexes.size());
    Index dropped = mocks.createdIndexes.get(0).getIndex();
    verify(dropped).drop();

    // Should have written an updated config journal with no deleted indexes.
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEmpty();
  }

  @Test
  public void testOneVectorIndexToDropIsDropped() throws Exception {
    var config =
        ConfigJournalV1Builder.builder()
            .deletedIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION)
            .build();

    ConfigStateMocks mocks = initializeWithConfig(config);

    // Shouldn't have tried to create any indexes.
    mocks.assertNoIndexCatalogedAndReplicated();
    mocks.assertOneIndexCreated();
    mocks.assertIndexCreated(MOCK_VECTOR_DEFINITION);

    // Make sure that the index was actually dropped
    Assert.assertEquals(1, mocks.createdIndexes.size());
    Index dropped = mocks.createdIndexes.get(0).getIndex();
    verify(dropped).drop();

    // Should have written an updated config journal with no deleted indexes.
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEmpty();
  }

  @Test
  public void testInitExistingSearchIndex() throws Exception {
    var config =
        ConfigJournalV1Builder.builder().liveIndex(MOCK_INDEX_DEFINITION_GENERATION).build();

    ConfigStateMocks mocks = initializeWithConfig(config);

    // Should create our one index
    mocks.assertOneIndexCreated();
    mocks.assertIndexCreated(MOCK_INDEX_DEFINITION_GENERATION);
    mocks.assertAtLeastOneIndexCataloged();

    // No replication should have taken place
    mocks.assertNoIndexReplicated();

    // Should have written an updated config journal - unchanged
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEquals(config);

    // Index should be in catalog
    Assert.assertEquals(1, mocks.indexCatalog.getIndexes().size());
    Assert.assertTrue(mocks.indexCatalog.getIndexById(MOCK_INDEX_ID).isPresent());
  }

  @Test
  public void testInitExistingVectorIndex() throws Exception {
    var config =
        ConfigJournalV1Builder.builder().liveIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION).build();

    ConfigStateMocks mocks = initializeWithConfig(config);

    // Should create our one index
    mocks.assertOneIndexCreated();
    mocks.assertIndexCreated(MOCK_VECTOR_INDEX_DEFINITION_GENERATION);
    mocks.assertAtLeastOneIndexCataloged();

    // No replication should have taken place
    mocks.assertNoIndexReplicated();

    // Should have written an updated config journal - unchanged
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEquals(config);

    // Index should be in catalog
    Assert.assertEquals(1, mocks.indexCatalog.getIndexes().size());
    Assert.assertTrue(mocks.indexCatalog.getIndexById(VectorIndex.MOCK_INDEX_ID).isPresent());
  }

  @Test
  public void testInitStagedSearchIndex() throws Exception {
    // we need to have a live index corresponding to the staged one to satisfy invariants:
    var stagedDefinition =
        MOCK_INDEX_DEFINITION_GENERATION.incrementUser(
            MOCK_INDEX_DEFINITION_GENERATION.definition());
    var config =
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_INDEX_DEFINITION_GENERATION)
            .stagedIndex(stagedDefinition)
            .build();

    ConfigStateMocks mocks = initializeWithConfig(config);

    // Should create our one index
    mocks.assertAtLeastOneIndexStaged();

    // No replication should have taken place
    mocks.assertNoIndexReplicated();

    // Should have written an updated config journal - unchanged
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEquals(config);

    // Index should be in catalog
    Assert.assertEquals(1, mocks.staged.getIndexes().size());
    Assert.assertTrue(mocks.staged.getIndex(MOCK_INDEX_ID).isPresent());
  }

  @Test
  public void testInitStagedVectorIndex() throws Exception {
    // we need to have a live index corresponding to the staged one to satisfy invariants:
    var stagedDefinition =
        MOCK_VECTOR_INDEX_DEFINITION_GENERATION.incrementUser(
            MOCK_VECTOR_INDEX_DEFINITION_GENERATION.definition());
    var config =
        ConfigJournalV1Builder.builder()
            .liveIndex(MOCK_VECTOR_INDEX_DEFINITION_GENERATION)
            .stagedIndex(stagedDefinition)
            .build();

    ConfigStateMocks mocks = initializeWithConfig(config);

    // Should create our one index
    mocks.assertAtLeastOneIndexStaged();

    // No replication should have taken place
    mocks.assertNoIndexReplicated();

    // Should have written an updated config journal - unchanged
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEquals(config);

    // Index should be in catalog
    Assert.assertEquals(1, mocks.staged.getIndexes().size());
    Assert.assertTrue(mocks.staged.getIndex(VectorIndex.MOCK_INDEX_ID).isPresent());
  }

  @Test
  public void testInitIndexAndDeletedWithSharedOverriddenAnalyzer() throws Exception {
    OverriddenBaseAnalyzerDefinition myAnalyzer =
        OverriddenBaseAnalyzerDefinitionBuilder.builder()
            .baseAnalyzerName("lucene.standard")
            .name("myAnalyzer")
            .build();

    var builder =
        SearchIndexDefinitionBuilder.builder()
            .defaultMetadata()
            .analyzerName("myAnalyzer")
            .dynamicMapping();
    var def1 =
        SearchIndexDefinitionGenerationBuilder.create(
            builder.indexId(new ObjectId()).name("def1").build(),
            Generation.FIRST,
            List.of(myAnalyzer));
    var def2 =
        SearchIndexDefinitionGenerationBuilder.create(
            builder.indexId(new ObjectId()).name("def2").build(),
            Generation.FIRST,
            List.of(myAnalyzer));
    var deletedDef =
        SearchIndexDefinitionGenerationBuilder.create(
            builder.indexId(new ObjectId()).name("def3").build(),
            Generation.FIRST,
            List.of(myAnalyzer));

    var config =
        ConfigJournalV1Builder.builder()
            .liveIndex(def1)
            .liveIndex(def2)
            .deletedIndex(deletedDef)
            .build();
    var configWithoutDeleted =
        ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();

    ConfigStateMocks mocks = initializeWithConfig(config);
    // Should create two indexes, and dropped the third
    mocks.assertAtLeastOneIndexCataloged();
    mocks.assertIndexCreated(def1);
    mocks.assertIndexCreated(def2);
    mocks.assertIndexCreatedAndDropped(deletedDef);
    // Should have written an updated config journal, without the deleted index.
    mocks.assertJournalPersistedAtLeastOnce();
    mocks.assertPersistedJournalEquals(configWithoutDeleted);
    // No replication should have taken place
    mocks.assertNoIndexReplicated();

    // Index should be in catalog
    Assert.assertEquals(2, mocks.indexCatalog.getIndexes().size());
    Assert.assertTrue(
        mocks.indexCatalog.getIndexById(def1.getIndexDefinition().getIndexId()).isPresent());
    Assert.assertTrue(
        mocks.indexCatalog.getIndexById(def2.getIndexDefinition().getIndexId()).isPresent());
    Assert.assertTrue(
        mocks.indexCatalog.getIndexById(deletedDef.getIndexDefinition().getIndexId()).isEmpty());
  }

  @Test
  public void testInitializerChecksSearchIndexInvariantUniqueIds() throws Exception {
    var id = new ObjectId();
    var builder = SearchIndexDefinitionBuilder.builder().dynamicMapping();
    var def1 =
        builder
            .indexId(id)
            .name("a")
            .database("b")
            .lastObservedCollectionName("c")
            .collectionUuid(UUID.randomUUID())
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();
    var def2 =
        builder
            .indexId(id)
            .name("1")
            .database("2")
            .lastObservedCollectionName("3")
            .collectionUuid(UUID.randomUUID())
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();
    var nonUniqueIndexId = ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(nonUniqueIndexId);
  }

  @Test
  public void testInitializerChecksVectorIndexInvariantUniqueIds() throws Exception {
    var id = new ObjectId();
    var builder = VectorIndexDefinitionBuilder.builder().withCosineVectorField("vector", 1024);
    var def1 =
        new VectorIndexDefinitionGeneration(
            builder
                .indexId(id)
                .name("a")
                .database("b")
                .lastObservedCollectionName("c")
                .collectionUuid(UUID.randomUUID())
                .build(),
            Generation.FIRST);
    var def2 =
        new VectorIndexDefinitionGeneration(
            builder
                .indexId(id)
                .name("1")
                .database("2")
                .lastObservedCollectionName("3")
                .collectionUuid(UUID.randomUUID())
                .build(),
            Generation.FIRST);

    var nonUniqueIndexId = ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(nonUniqueIndexId);
  }

  @Test
  public void testInitializerChecksIndexInvariantUniqueIdsBetweenSearchAndVectorIndexes()
      throws Exception {
    var id = new ObjectId();
    var def1 =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.builder()
                .withCosineVectorField("vector", 1024)
                .indexId(id)
                .name("a")
                .database("b")
                .lastObservedCollectionName("c")
                .collectionUuid(UUID.randomUUID())
                .build(),
            Generation.FIRST);
    var def2 =
        SearchIndexDefinitionBuilder.builder()
            .dynamicMapping()
            .indexId(id)
            .name("1")
            .database("2")
            .lastObservedCollectionName("3")
            .collectionUuid(UUID.randomUUID())
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();

    var nonUniqueIndexId = ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(nonUniqueIndexId);
  }

  @Test
  public void testInitializerChecksSearchIndexInvariantUniqueNamespace() throws Exception {
    // both indexes have the same namespace but different id
    var def1 =
        SearchIndexDefinitionBuilder.builder()
            .dynamicMapping()
            .defaultMetadata()
            .indexId(new ObjectId())
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();
    var def2 =
        SearchIndexDefinitionBuilder.from(def1.getIndexDefinition())
            .indexId(new ObjectId())
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();
    var nonUniqueIndexId = ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(nonUniqueIndexId);
  }

  @Test
  public void testInitializerChecksVectorIndexInvariantUniqueNamespace() throws Exception {
    // both indexes have the same namespace but different id
    var def1 =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.builder()
                .withCosineVectorField("vector", 1024)
                .indexId(new ObjectId())
                .name("1")
                .database("2")
                .lastObservedCollectionName("3")
                .build(),
            Generation.FIRST);
    var def2 =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.from(def1.getIndexDefinition())
                .indexId(new ObjectId())
                .build(),
            Generation.FIRST);

    var nonUniqueIndexId = ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(nonUniqueIndexId);
  }

  @Test
  public void testInitializerChecksNonUniqueNamespacesBetweenSearchAndVectorIndexes()
      throws Exception {
    // both search and vector indexes have the same namespace but different id
    var def1 =
        SearchIndexDefinitionBuilder.builder()
            .dynamicMapping()
            .defaultMetadata()
            .name("1")
            .database("2")
            .lastObservedCollectionName("3")
            .collectionUuid(UUID.randomUUID())
            .indexId(new ObjectId())
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();
    var def2 =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.builder()
                .withCosineVectorField("vector", 1024)
                .indexId(new ObjectId())
                .name("1")
                .database("2")
                .lastObservedCollectionName("3")
                .collectionUuid(def1.getIndexDefinition().getCollectionUuid())
                .build(),
            Generation.FIRST);

    var nonUniqueIndexId = ConfigJournalV1Builder.builder().liveIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(nonUniqueIndexId);
  }

  @Test
  public void testInitializerChecksInvariantsBetweenStagedAndLiveSearchIndexes() throws Exception {
    var def1 =
        SearchIndexDefinitionBuilder.builder()
            .dynamicMapping()
            .defaultMetadata()
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();
    var def2 =
        SearchIndexDefinitionBuilder.from(def1.getIndexDefinition())
            .database("changed")
            .asDefinitionGeneration()
            .generation(Generation.FIRST)
            .build();
    var stagedIndexChangedNamespace =
        ConfigJournalV1Builder.builder().stagedIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(stagedIndexChangedNamespace);
  }

  @Test
  public void testInitializerChecksInvariantsBetweenStagedAndLiveVectorIndexes() throws Exception {

    var def1 =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.builder()
                .withCosineVectorField("vector", 1024)
                .indexId(new ObjectId())
                .name("1")
                .database("2")
                .lastObservedCollectionName("3")
                .build(),
            Generation.FIRST);
    var def2 =
        new VectorIndexDefinitionGeneration(
            VectorIndexDefinitionBuilder.from(def1.getIndexDefinition())
                .database("changed")
                .build(),
            Generation.FIRST);

    var stagedIndexChangedNamespace =
        ConfigJournalV1Builder.builder().stagedIndex(def1).liveIndex(def2).build();
    assertInvariantViolationDetected(stagedIndexChangedNamespace);
  }

  /** Run initializer on this config journal, return the mocks acted upon. */
  private ConfigStateMocks initializeWithConfig(ConfigJournalV1 config) throws Exception {
    var mocks = ConfigStateMocks.create();
    ConfigInitializer.initialize(
        mocks.configState, config, Optional.empty(), new SimpleMeterRegistry());
    return mocks;
  }

  private void assertInvariantViolationDetected(ConfigJournalV1 config) throws Exception {
    var mocks = ConfigStateMocks.create();
    Assert.assertThrows(
        Invariants.InvariantException.class,
        () ->
            ConfigInitializer.initialize(
                mocks.configState, config, Optional.empty(), new SimpleMeterRegistry()));
    mocks.assertNoIndexActivity();
  }
}
