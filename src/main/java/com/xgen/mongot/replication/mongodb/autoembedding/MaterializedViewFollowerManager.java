package com.xgen.mongot.replication.mongodb.autoembedding;

import static com.xgen.mongot.replication.mongodb.autoembedding.MaterializedViewManager.getCollectionUuid;
import static com.xgen.mongot.util.Check.checkState;
import static com.xgen.mongot.util.FutureUtils.COMPLETED_FUTURE;

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.xgen.mongot.embedding.mongodb.leasing.LeaseManager;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.autoembedding.AutoEmbeddingIndexGeneration;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.metrics.MeterAndFtdcRegistry;
import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.VerboseRunnable;
import com.xgen.mongot.util.concurrent.Executors;
import com.xgen.mongot.util.concurrent.NamedScheduledExecutorService;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Singleton instance created at startup, manages materialized view status tracking for follower
 * nodes.
 */
public class MaterializedViewFollowerManager implements ReplicationManager {

  private static final Logger LOG = LoggerFactory.getLogger(MaterializedViewFollowerManager.class);

  // TODO(CLOUDP-356241): Make this parameter part of materializedViewManagerConfig
  private static final Duration DEFAULT_STATUS_TRACKING_INTERVAL = Duration.ofSeconds(30);

  private final SyncSourceConfig syncSourceConfig;

  private final MeterRegistry meterRegistry;

  private final LeaseManager leaseManager;

  /**
   * A mapping of auto-embedding index generation IDs to active MaterializedViewGenerations, e.g.,
   * {'123_u1_f1_a1': matview_123_u1, '123_u1_f1_a2': matview_123_u1}
   */
  @VisibleForTesting
  final Map<GenerationId, MaterializedViewIndexGeneration> genIdToActiveMatViewGens;

  /**
   * A mapping of active materialized view generations by IndexID. We expect this data structure to
   * be in sync with materializedViewIndexCatalog.
   */
  @VisibleForTesting
  final Map<UUID, MaterializedViewIndexGeneration> activeMaterializedViewGenerations;

  private final NamedScheduledExecutorService scheduledExecutorService;

  private final ScheduledFuture<?> statusTrackerFuture;

  @GuardedBy("this")
  private boolean shutdown;

  @VisibleForTesting
  MaterializedViewFollowerManager(
      SyncSourceConfig syncSourceConfig,
      NamedScheduledExecutorService scheduledExecutorService,
      MeterRegistry meterRegistry,
      LeaseManager leaseManager) {
    this.meterRegistry = meterRegistry;
    this.genIdToActiveMatViewGens = new ConcurrentHashMap<>();
    this.activeMaterializedViewGenerations = new ConcurrentHashMap<>();
    this.scheduledExecutorService = scheduledExecutorService;
    this.syncSourceConfig = syncSourceConfig;
    this.leaseManager = leaseManager;
    this.shutdown = false;
    this.statusTrackerFuture =
        scheduledExecutorService.scheduleWithFixedDelay(
            new VerboseRunnable() {
              @Override
              public void verboseRun() {
                refreshStatuses();
              }

              @Override
              public Logger getLogger() {
                return LOG;
              }
            },
            0,
            DEFAULT_STATUS_TRACKING_INTERVAL.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  /**
   * Creates a new MaterializedViewFollowerManager by syncSourceConfig, used for follower nodes to
   * retrieve auto-embedding materialized view status for all managed
   * MaterializedViewIndexGenerations.
   */
  public static MaterializedViewFollowerManager create(
      SyncSourceConfig syncSourceConfig,
      MeterAndFtdcRegistry meterAndFtdcRegistry,
      LeaseManager leaseManager) {
    LOG.info("creating MaterializedViewFollowerManager");
    var meterRegistry = meterAndFtdcRegistry.meterRegistry();
    var executor =
        Executors.singleThreadScheduledExecutor("mat-view-status-tracker", meterRegistry);
    return new MaterializedViewFollowerManager(
        syncSourceConfig, executor, meterRegistry, leaseManager);
  }

  @Override
  public Optional<SyncSourceConfig> getSyncSourceConfig() {
    return Optional.of(this.syncSourceConfig);
  }

  @Override
  public synchronized void add(IndexGeneration indexGeneration) {
    checkState(!this.shutdown, "cannot call add() after shutdown()");
    AutoEmbeddingIndexGeneration autoEmbeddingIndexGeneration =
        Check.instanceOf(indexGeneration, AutoEmbeddingIndexGeneration.class);
    MaterializedViewIndexGeneration matViewIndexGeneration =
        autoEmbeddingIndexGeneration.getMaterializedViewIndexGeneration();
    UUID uuid = getCollectionUuid(autoEmbeddingIndexGeneration.getGenerationId());
    this.activeMaterializedViewGenerations.compute(
        uuid,
        (indexId, activeGeneration) -> {
          if (activeGeneration == null) {
            this.genIdToActiveMatViewGens.put(
                autoEmbeddingIndexGeneration.getGenerationId(), matViewIndexGeneration);
            return matViewIndexGeneration;
          } else if (activeGeneration.needsNewMatViewGenerator(matViewIndexGeneration)) {
            this.genIdToActiveMatViewGens.put(
                autoEmbeddingIndexGeneration.getGenerationId(), matViewIndexGeneration);
            return matViewIndexGeneration;
          } else {
            this.genIdToActiveMatViewGens.put(
                autoEmbeddingIndexGeneration.getGenerationId(), activeGeneration);
            matViewIndexGeneration.swapIndex(activeGeneration.getIndex());
            return activeGeneration;
          }
        });
  }

  @Override
  public synchronized CompletableFuture<Void> dropIndex(GenerationId generationId) {
    checkState(!this.shutdown, "cannot call dropIndex() after shutdown()");
    this.genIdToActiveMatViewGens.remove(generationId);
    // remove from activeMaterializedViewGenerations if no other generation is using the same UUID
    UUID uuid = getCollectionUuid(generationId);
    if (this.genIdToActiveMatViewGens.keySet().stream()
        .map(MaterializedViewManager::getCollectionUuid)
        .filter(uuid::equals)
        .findFirst()
        .isEmpty()) {
      this.activeMaterializedViewGenerations.remove(uuid);
    }
    return COMPLETED_FUTURE;
  }

  @Override
  public synchronized CompletableFuture<Void> shutdown() {
    if (this.shutdown) {
      LOG.info("Already shutdown");
      return COMPLETED_FUTURE;
    }
    LOG.info("Shutting down.");
    this.shutdown = true;

    // Need to create a separate executor to run the shutdown tasks, otherwise it may end up running
    // on the indexing executor. As one of the shutdown tasks is shutting down that executor, this
    // will hang forever.
    var shutdownExecutor =
        Executors.fixedSizeThreadPool("mat-view-manager-shutdown", 1, this.meterRegistry);
    this.statusTrackerFuture.cancel(false);
    // Replace COMPLETED_FUTURE with CompletableFuture.runAsync(this.leaseManager::close,
    // shutdownExecutor)
    return COMPLETED_FUTURE
        .thenRunAsync(
            () -> Executors.shutdownOrFail(this.scheduledExecutorService), shutdownExecutor)
        // Signal the shutdown executor to clean up, but don't block waiting for it to do so.
        .thenRunAsync(shutdownExecutor::shutdown, shutdownExecutor);
  }

  @Override
  public boolean isReplicationSupported() {
    return false;
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  private synchronized void refreshStatuses() {
    if (this.shutdown) {
      return;
    }
    this.genIdToActiveMatViewGens.values().stream()
        .distinct()
        .forEach(
            materializedViewIndexGeneration -> {
              var status =
                  this.leaseManager.getMaterializedViewReplicationStatus(
                      materializedViewIndexGeneration);
              materializedViewIndexGeneration.getIndex().setStatus(status);
            });
  }
}
