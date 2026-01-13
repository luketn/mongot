package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewDefinitionGeneration;
import static com.xgen.testing.mongot.mock.index.MaterializedViewIndex.mockMatViewIndexGeneration;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.definition.MaterializedViewIndexDefinitionGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class MaterializedViewFollowerManagerTest {
  private static final MaterializedViewIndexDefinitionGeneration MOCK_INDEX_DEFINITION_GENERATION =
      mockMatViewDefinitionGeneration(new ObjectId());

  @Test
  public void testAddIndex() {
    var meterRegistry = new SimpleMeterRegistry();
    var mockScheduler = mock(NamedScheduledExecutorService.class);
    var mockLeaseManager = mock(LeaseManager.class);
    var expectedStatus = IndexStatus.steady();
    when(mockLeaseManager.getMaterializedViewReplicationStatus(any())).thenReturn(expectedStatus);
    MaterializedViewFollowerManager manager =
        new MaterializedViewFollowerManager(
            mock(SyncSourceConfig.class), mockScheduler, meterRegistry, mockLeaseManager);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockScheduler)
        .scheduleWithFixedDelay(
            runnableCaptor.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    manager.add(autoEmbeddingIndexGeneration);
    runnableCaptor.getValue().run();
    verify(materializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(expectedStatus, materializedViewindexGeneration.getIndex().getStatus());
    assertEquals(1, manager.genIdToActiveMatViewGens.size());
    assertEquals(1, manager.activeMaterializedViewGenerations.size());
  }

  @Test
  public void testAddIndexNewIndexDefinitionVersion() {
    var meterRegistry = new SimpleMeterRegistry();
    var mockScheduler = mock(NamedScheduledExecutorService.class);
    var mockLeaseManager = mock(LeaseManager.class);
    var expectedStatus = IndexStatus.steady();
    when(mockLeaseManager.getMaterializedViewReplicationStatus(any())).thenReturn(expectedStatus);
    MaterializedViewFollowerManager manager =
        new MaterializedViewFollowerManager(
            mock(SyncSourceConfig.class), mockScheduler, meterRegistry, mockLeaseManager);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockScheduler)
        .scheduleWithFixedDelay(
            runnableCaptor.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    manager.add(autoEmbeddingIndexGeneration);

    MaterializedViewIndexGeneration newMaterializedViewindexGeneration =
        mockMatViewIndexGeneration(
            mockMatViewDefinitionGeneration(materializedViewindexGeneration.getGenerationId(), 1));
    AutoEmbeddingIndexGeneration newAutoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(newAutoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(newMaterializedViewindexGeneration);
    when(newAutoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(
            newMaterializedViewindexGeneration
                .getDefinitionGeneration()
                .incrementUser()
                .getGenerationId());
    manager.add(newAutoEmbeddingIndexGeneration);

    runnableCaptor.getValue().run();
    verify(materializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    verify(newMaterializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(expectedStatus, materializedViewindexGeneration.getIndex().getStatus());
    assertEquals(expectedStatus, newMaterializedViewindexGeneration.getIndex().getStatus());
    assertEquals(2, manager.genIdToActiveMatViewGens.size());
    assertEquals(1, manager.activeMaterializedViewGenerations.size());
  }

  @Test
  public void testAddIndexSameIndexDefinitionVersion() {
    var meterRegistry = new SimpleMeterRegistry();
    var mockScheduler = mock(NamedScheduledExecutorService.class);
    var mockLeaseManager = mock(LeaseManager.class);
    var expectedStatus = IndexStatus.steady();
    when(mockLeaseManager.getMaterializedViewReplicationStatus(any())).thenReturn(expectedStatus);
    MaterializedViewFollowerManager manager =
        new MaterializedViewFollowerManager(
            mock(SyncSourceConfig.class), mockScheduler, meterRegistry, mockLeaseManager);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockScheduler)
        .scheduleWithFixedDelay(
            runnableCaptor.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    manager.add(autoEmbeddingIndexGeneration);

    MaterializedViewIndexGeneration newMaterializedViewindexGeneration =
        mockMatViewIndexGeneration(
            mockMatViewDefinitionGeneration(materializedViewindexGeneration.getGenerationId()));
    AutoEmbeddingIndexGeneration newAutoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(newAutoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(newMaterializedViewindexGeneration);
    when(newAutoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(
            newMaterializedViewindexGeneration
                .getDefinitionGeneration()
                .incrementUser()
                .getGenerationId());
    manager.add(newAutoEmbeddingIndexGeneration);

    runnableCaptor.getValue().run();
    verify(materializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(expectedStatus, materializedViewindexGeneration.getIndex().getStatus());
    assertEquals(2, manager.genIdToActiveMatViewGens.size());
    assertEquals(1, manager.activeMaterializedViewGenerations.size());
  }

  @Test
  public void testDropIndex() {
    var meterRegistry = new SimpleMeterRegistry();
    var mockScheduler = mock(NamedScheduledExecutorService.class);
    var mockLeaseManager = mock(LeaseManager.class);
    var expectedStatus = IndexStatus.steady();
    when(mockLeaseManager.getMaterializedViewReplicationStatus(any())).thenReturn(expectedStatus);
    MaterializedViewFollowerManager manager =
        new MaterializedViewFollowerManager(
            mock(SyncSourceConfig.class), mockScheduler, meterRegistry, mockLeaseManager);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockScheduler)
        .scheduleWithFixedDelay(
            runnableCaptor.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    manager.add(autoEmbeddingIndexGeneration);
    runnableCaptor.getValue().run();
    verify(materializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(expectedStatus, materializedViewindexGeneration.getIndex().getStatus());
    assertEquals(1, manager.genIdToActiveMatViewGens.size());
    assertEquals(1, manager.activeMaterializedViewGenerations.size());

    manager.dropIndex(autoEmbeddingIndexGeneration.getGenerationId());
    assertEquals(0, manager.genIdToActiveMatViewGens.size());
    assertEquals(0, manager.activeMaterializedViewGenerations.size());
  }

  @Test
  public void testDropIndexNewIndexDefinitionVersion() {
    var meterRegistry = new SimpleMeterRegistry();
    var mockScheduler = mock(NamedScheduledExecutorService.class);
    var mockLeaseManager = mock(LeaseManager.class);
    var expectedStatus = IndexStatus.steady();
    when(mockLeaseManager.getMaterializedViewReplicationStatus(any())).thenReturn(expectedStatus);
    MaterializedViewFollowerManager manager =
        new MaterializedViewFollowerManager(
            mock(SyncSourceConfig.class), mockScheduler, meterRegistry, mockLeaseManager);
    ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(mockScheduler)
        .scheduleWithFixedDelay(
            runnableCaptor.capture(), anyLong(), anyLong(), eq(TimeUnit.MILLISECONDS));
    MaterializedViewIndexGeneration materializedViewindexGeneration =
        mockMatViewIndexGeneration(MOCK_INDEX_DEFINITION_GENERATION);
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(materializedViewindexGeneration);
    when(autoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(materializedViewindexGeneration.getGenerationId());
    manager.add(autoEmbeddingIndexGeneration);

    MaterializedViewIndexGeneration newMaterializedViewindexGeneration =
        mockMatViewIndexGeneration(
            mockMatViewDefinitionGeneration(materializedViewindexGeneration.getGenerationId(), 1));
    AutoEmbeddingIndexGeneration newAutoEmbeddingIndexGeneration =
        mock(AutoEmbeddingIndexGeneration.class);
    when(newAutoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration())
        .thenReturn(newMaterializedViewindexGeneration);
    when(newAutoEmbeddingIndexGeneration.getGenerationId())
        .thenReturn(
            newMaterializedViewindexGeneration
                .getDefinitionGeneration()
                .incrementUser()
                .getGenerationId());
    manager.add(newAutoEmbeddingIndexGeneration);

    runnableCaptor.getValue().run();
    verify(materializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    verify(newMaterializedViewindexGeneration.getIndex()).setStatus(any(IndexStatus.class));
    assertEquals(expectedStatus, materializedViewindexGeneration.getIndex().getStatus());
    assertEquals(expectedStatus, newMaterializedViewindexGeneration.getIndex().getStatus());
    assertEquals(2, manager.genIdToActiveMatViewGens.size());
    assertEquals(1, manager.activeMaterializedViewGenerations.size());

    manager.dropIndex(autoEmbeddingIndexGeneration.getGenerationId());
    assertEquals(1, manager.genIdToActiveMatViewGens.size());
    assertEquals(1, manager.activeMaterializedViewGenerations.size());

    manager.dropIndex(newAutoEmbeddingIndexGeneration.getGenerationId());
    assertEquals(0, manager.genIdToActiveMatViewGens.size());
    assertEquals(0, manager.activeMaterializedViewGenerations.size());
  }
}
