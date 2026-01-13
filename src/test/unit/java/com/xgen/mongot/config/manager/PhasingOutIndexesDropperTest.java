package com.xgen.mongot.config.manager;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.xgen.mongot.config.backup.ConfigJournalV1;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.testing.mongot.config.backup.ConfigJournalV1Builder;
import com.xgen.testing.mongot.config.manager.ConfigStateMocks;
import java.util.Collections;
import java.util.List;
import org.bson.types.ObjectId;
import org.junit.Assert;
import org.junit.Test;

public class PhasingOutIndexesDropperTest {
  @Test
  public void testNoPhaseOutDoesNothing() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    dropUnused(mocks);

    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
  }

  @Test
  public void testPhaseOutWithOpenCursorNotDropped() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();

    var indexGen = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);
    InitializedIndex initializedIndex =
        mocks.waitAndGetInitializedIndex(indexGen.getGenerationId());
    openCursor(mocks, indexGen);
    mocks.clearInvocations();

    dropUnused(mocks);

    mocks.assertNoIndexActivity();
    verifyNoMoreInteractions(mocks.journalWriter);
    verify(initializedIndex, times(0)).close();
    verify(indexGen.getIndex(), times(0)).drop();
  }

  @Test
  public void testPhaseOutDropsIndex() throws Exception {
    ConfigStateMocks mocks = getEmptyMocks();
    var indexGeneration = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);
    var index = indexGeneration.getIndex();
    InitializedIndex initializedIndex =
        mocks.waitAndGetInitializedIndex(indexGeneration.getGenerationId());
    mocks.clearInvocations();

    dropUnused(mocks);

    // make sure that we:
    // 1. stop replication
    // 2. drop the index
    // 3. remove the index from the journal
    var inOrder = inOrder(mocks.lifecycleManager, mocks.journalWriter, index, initializedIndex);
    inOrder.verify(mocks.lifecycleManager).dropIndex(indexGeneration.getGenerationId());
    inOrder.verify(initializedIndex).close();
    inOrder.verify(index).drop();
    inOrder
        .verify(mocks.journalWriter)
        .persist(argThat(journal -> emptyConfigJournal().equals(journal)));

    mocks.assertPersistedJournalEmpty();
    // index should be removed from phasingOut
    Assert.assertEquals(Collections.emptyList(), mocks.configState.phasingOut.getIndexes());
    // nothing should be on the journal
    mocks.assertPersistedJournalEmpty();
  }

  /**
   * Test that when we drop an index, other indexes in other states should not be modified and
   * should be journaled correctly.
   */
  @Test
  public void testPhaseOutDoesNotCorruptJournalForOtherStates() throws Exception {
    var mocks = ConfigStateMocks.create();

    var liveIndex = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.LIVE);
    InitializedIndex initializedIndex =
        mocks.waitAndGetInitializedIndex(liveIndex.getGenerationId());
    var liveDefinitionGeneration = liveIndex.getDefinitionGeneration().asSearch();
    var stagedIndex =
        mocks.addIndex(
            liveDefinitionGeneration.incrementUser(liveDefinitionGeneration.definition()),
            ConfigStateMocks.State.STAGED);
    var toPhaseOut = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);
    var notReadyToPhaseOut = mocks.addIndex(new ObjectId(), ConfigStateMocks.State.PHASE_OUT);

    InitializedIndex phaseOutInitializedIndex =
        mocks.waitAndGetInitializedIndex(toPhaseOut.getGenerationId());

    InitializedIndex notReadyToPhaseOutInitializedIndex =
        mocks.waitAndGetInitializedIndex(notReadyToPhaseOut.getGenerationId());

    openCursor(mocks, notReadyToPhaseOut);
    mocks.clearInvocations();

    dropUnused(mocks);
    // one index should be dropped
    verify(phaseOutInitializedIndex).close();
    verify(toPhaseOut.getIndex(), atLeastOnce()).drop();

    verify(notReadyToPhaseOutInitializedIndex, times(0)).close();
    verify(notReadyToPhaseOut.getIndex(), never()).drop();
    Assert.assertEquals(List.of(notReadyToPhaseOut), mocks.phasingOut.getIndexes());

    mocks.assertStagedIndexesAre(stagedIndex);
    mocks.assertLiveIndexesAre(liveIndex);

    verify(initializedIndex, never()).close();
    verify(liveIndex.getIndex(), never()).drop();

    // journal should record the other indexes
    ConfigJournalV1 expectedJournal =
        ConfigJournalV1Builder.builder()
            .stagedIndex(stagedIndex.getDefinitionGeneration())
            .liveIndex(liveIndex.getDefinitionGeneration())
            .deletedIndex(notReadyToPhaseOut.getDefinitionGeneration())
            .build();
    mocks.assertPersistedJournalEquals(expectedJournal);
  }

  private void dropUnused(ConfigStateMocks mocks) throws Exception {
    PhasingOutIndexesDropper.dropUnused(
        mocks.configState, IndexActions.withReplication(mocks.configState));
  }

  private void openCursor(ConfigStateMocks mocks, IndexGeneration index) {
    when(mocks.cursorManager.hasOpenCursors(index.getGenerationId())).thenReturn(true);
  }

  private ConfigJournalV1 emptyConfigJournal() {
    return ConfigJournalV1Builder.builder().build();
  }

  private ConfigStateMocks getEmptyMocks() throws Exception {
    return ConfigStateMocks.create();
  }
}
