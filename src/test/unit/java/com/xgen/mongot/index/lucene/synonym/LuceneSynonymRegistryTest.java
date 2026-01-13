package com.xgen.mongot.index.lucene.synonym;

import static org.mockito.Mockito.mock;

import com.pholser.junit.quickcheck.random.SourceOfRandomness;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.status.SynonymStatus;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymMappingException;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.util.functionalinterfaces.CheckedRunnable;
import com.xgen.testing.mongot.index.analyzer.AnalyzerRegistryBuilder;
import com.xgen.testing.mongot.index.definition.SynonymMappingDefinitionBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiConsumer;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneSynonymRegistryTest {

  @Test
  public void testCreate() throws Exception {
    String mappingName = "mappingName";
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    var registryEntry = createMappingDefinition(mappingName);

    LuceneSynonymRegistry registry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, Map.ofEntries(registryEntry), Optional.empty());

    Assert.assertSame(SynonymStatus.SYNC_ENQUEUED, registry.getStatuses().get(mappingName));
    checkThrowsWithType(
        () -> registry.get(mappingName), SynonymMappingException.Type.MAPPING_NOT_READY);
  }

  @Test
  public void testUpdate() throws Exception {
    String mappingName = "mappingName";
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    var registryEntry = createMappingDefinition(mappingName);

    LuceneSynonymRegistry registry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, Map.ofEntries(registryEntry), Optional.empty());

    SynonymMapping synonymMapping = mock(SynonymMapping.class);
    registry.update(mappingName, synonymMapping);

    Assert.assertSame(SynonymStatus.READY, registry.getStatuses().get(mappingName));
    Assert.assertSame(synonymMapping, registry.get(mappingName));
  }

  @Test
  public void testGetUnknownMapping() throws Exception {
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();

    LuceneSynonymRegistry registry =
        LuceneSynonymRegistry.create(analyzerRegistry, Collections.emptyMap(), Optional.empty());

    checkThrowsWithType(
        () -> registry.get("myMapping"), SynonymMappingException.Type.UNKNOWN_MAPPING);
  }

  @Test
  public void testGetNotReadyMapping() throws Exception {
    String mappingName = "synonymMappingName";
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    var registryEntry = createMappingDefinition(mappingName);

    LuceneSynonymRegistry registry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, Map.ofEntries(registryEntry), Optional.empty());

    Runnable checkThrowsNotReady =
        () ->
            checkThrowsWithType(
                () -> registry.get(mappingName), SynonymMappingException.Type.MAPPING_NOT_READY);

    // Not ready after synonym registry instantiation.
    checkThrowsNotReady.run();

    // Not ready after first update begins.
    registry.beginUpdate(mappingName);
    checkThrowsNotReady.run();

    // Not ready after seeing invalid doc.
    registry.invalidate(mappingName, "");
    checkThrowsNotReady.run();

    // Not ready after seeing change from invalid.
    registry.observeChange(mappingName);
    checkThrowsNotReady.run();

    // Not ready after starting sync from invalid state.
    registry.beginUpdate(mappingName);
    checkThrowsNotReady.run();

    // Not ready after failing or shutting down before sync attempt completes.
    registry.fail(mappingName, "");
    checkThrowsNotReady.run();
  }

  @Test
  public void testMappingStateTransitions() throws Exception {
    String mappingName = "synonymMappingName";
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    var registryEntry = createMappingDefinition(mappingName);

    LuceneSynonymRegistry registry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, Map.ofEntries(registryEntry), Optional.empty());

    // SYNC_ENQUEUED after synonym registry instantiation.
    Assert.assertSame(SynonymStatus.SYNC_ENQUEUED, registry.getStatuses().get(mappingName));

    // INITIAL_SYNC after first update begins.
    registry.beginUpdate(mappingName);
    Assert.assertSame(SynonymStatus.INITIAL_SYNC, registry.getStatuses().get(mappingName));

    // INVALID after seeing invalid doc.
    registry.invalidate(mappingName, "");
    Assert.assertSame(SynonymStatus.INVALID, registry.getStatuses().get(mappingName));

    // INITIAL_SYNC after seeing change from invalid state.
    registry.beginUpdate(mappingName);
    Assert.assertSame(SynonymStatus.INITIAL_SYNC, registry.getStatuses().get(mappingName));

    // READY after seeing collection drop.
    registry.clear(registryEntry.getValue());
    Assert.assertSame(SynonymStatus.READY, registry.getStatuses().get(mappingName));

    // READY_UPDATING after seeing change in ready state.
    registry.observeChange(mappingName);
    Assert.assertSame(SynonymStatus.READY_UPDATING, registry.getStatuses().get(mappingName));

    // INVALID after seeing invalid doc from any state.
    registry.invalidate(mappingName, "");
    Assert.assertSame(SynonymStatus.INVALID, registry.getStatuses().get(mappingName));

    // FAILED after failing or shutting down before sync attempt completes.
    registry.fail(mappingName, "");
    Assert.assertSame(SynonymStatus.FAILED, registry.getStatuses().get(mappingName));
  }

  @Test
  public void assertStateTransitionInvariantsHold() throws Exception {
    String mappingName = "synonymMappingName";
    AnalyzerRegistry analyzerRegistry = AnalyzerRegistryBuilder.empty();
    var registryEntry = createMappingDefinition(mappingName);

    LuceneSynonymRegistry registry =
        LuceneSynonymRegistry.create(
            analyzerRegistry, Map.ofEntries(registryEntry), Optional.empty());

    StateTransitionInvariants.run(1000, registry, registryEntry.getValue());
  }

  private static void checkThrowsWithType(
      CheckedRunnable<SynonymMappingException> runnable, SynonymMappingException.Type type) {
    SynonymMappingException e = Assert.assertThrows(SynonymMappingException.class, runnable::run);
    Assert.assertSame(type, e.type);
  }

  private static Map.Entry<String, SynonymMappingDefinition> createMappingDefinition(String name) {
    return Map.entry(
        name,
        SynonymMappingDefinitionBuilder.builder()
            .name(name)
            .synonymSourceDefinition(String.format("syn-coll-%s", name))
            .analyzer("lucene.standard")
            .build());
  }

  private static class StateTransitionInvariants {
    private static final Logger LOG = LoggerFactory.getLogger(StateTransitionInvariants.class);

    private static final List<BiConsumer<SynonymRegistry, SynonymMappingDefinition>>
        TRANSITION_INVARIANTS =
            List.of(
                StateTransitionInvariants::checkInvalidAfterInvalidate,
                StateTransitionInvariants::checkReadyAfterDrop,
                StateTransitionInvariants::checkReadyAfterUpdate,
                StateTransitionInvariants::checkReadyUpdatingChangeNoticedWhenReady,
                StateTransitionInvariants::checkSyncEnqueuedInvalidChangeNoticed,
                StateTransitionInvariants::checkInitialSyncUpdateBeginFromInvalid,
                StateTransitionInvariants::checkReadyUpdatingUpdateBeginFromReady);

    private final SourceOfRandomness random;
    private final int numTransitions;

    private StateTransitionInvariants(SourceOfRandomness random, int numTransitions) {
      this.random = random;
      LOG.info("random seed set to {}", this.random.seed());

      this.numTransitions = numTransitions;
    }

    private void run(SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      for (int i = 0; i != this.numTransitions; i++) {
        TRANSITION_INVARIANTS
            .get(this.random.nextInt(0, TRANSITION_INVARIANTS.size() - 1))
            .accept(registry, mappingDefinition);
      }
    }

    static void run(int numRuns, SynonymRegistry registry, SynonymMappingDefinition definition)
        throws Exception {
      run(new SourceOfRandomness(new Random()), numRuns, registry, definition);
    }

    @SuppressWarnings("unused")
    static void run(
        int numRuns, long seed, SynonymRegistry registry, SynonymMappingDefinition definition)
        throws Exception {
      run(new SourceOfRandomness(new Random(seed)), numRuns, registry, definition);
    }

    private static void run(
        SourceOfRandomness random,
        int numRuns,
        SynonymRegistry registry,
        SynonymMappingDefinition definition) {
      StateTransitionInvariants invariants = new StateTransitionInvariants(random, numRuns);
      invariants.run(registry, definition);
    }

    private static void checkInvalidAfterInvalidate(
        SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      registry.invalidate(mappingDefinition.name(), "");
      Assert.assertSame(
          SynonymStatus.INVALID, registry.getStatuses().get(mappingDefinition.name()));
    }

    private static void checkReadyAfterDrop(
        SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      registry.clear(mappingDefinition);
      Assert.assertSame(SynonymStatus.READY, registry.getStatuses().get(mappingDefinition.name()));
    }

    private static void checkReadyAfterUpdate(
        SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      registry.update(mappingDefinition.name(), mock(SynonymMapping.class));
      Assert.assertSame(SynonymStatus.READY, registry.getStatuses().get(mappingDefinition.name()));
    }

    private static void checkReadyUpdatingChangeNoticedWhenReady(
        SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      registry.update(mappingDefinition.name(), mock(SynonymMapping.class));
      registry.observeChange(mappingDefinition.name());
      Assert.assertSame(
          SynonymStatus.READY_UPDATING, registry.getStatuses().get(mappingDefinition.name()));
    }

    private static void checkSyncEnqueuedInvalidChangeNoticed(
        SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      registry.invalidate(mappingDefinition.name(), "");
      registry.observeChange(mappingDefinition.name());
      Assert.assertSame(
          SynonymStatus.SYNC_ENQUEUED, registry.getStatuses().get(mappingDefinition.name()));
    }

    private static void checkInitialSyncUpdateBeginFromInvalid(
        SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      registry.invalidate(mappingDefinition.name(), "");
      registry.observeChange(mappingDefinition.name());
      registry.beginUpdate(mappingDefinition.name());
      Assert.assertSame(
          SynonymStatus.INITIAL_SYNC, registry.getStatuses().get(mappingDefinition.name()));
    }

    private static void checkReadyUpdatingUpdateBeginFromReady(
        SynonymRegistry registry, SynonymMappingDefinition mappingDefinition) {
      registry.update(mappingDefinition.name(), mock(SynonymMapping.class));
      registry.observeChange(mappingDefinition.name());
      registry.beginUpdate(mappingDefinition.name());
      Assert.assertSame(
          SynonymStatus.READY_UPDATING, registry.getStatuses().get(mappingDefinition.name()));
    }
  }
}
