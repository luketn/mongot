package com.xgen.mongot.replication.mongodb.autoembedding;


import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.featureflag.Feature;
import com.xgen.mongot.featureflag.FeatureFlags;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.autoembedding.MaterializedViewIndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.metrics.MetricsFactory;
import com.xgen.mongot.replication.mongodb.ReplicationIndexManager;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.PeriodicIndexCommitter;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.replication.mongodb.initialsync.InitialSyncQueue;
import com.xgen.mongot.replication.mongodb.steadystate.SteadyStateManager;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** MaterializedViewGenerator manages per index Mat View generation for auto-embedding index. */
public class MaterializedViewGenerator extends ReplicationIndexManager {

  MaterializedViewGenerator(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      MetricsFactory metricsFactory,
      FeatureFlags featureFlags,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoffMs,
      boolean enableNaturalOrderScan) {
    super(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        Collections.emptyList(),
        indexGeneration,
        initializedIndex,
        documentIndexer,
        periodicCommitter,
        metricsFactory,
        featureFlags,
        resyncBackoff,
        transientBackoff,
        requestRateLimitBackoffMs,
        enableNaturalOrderScan);
  }

  /**
   * Creates and initializes a MaterializedViewGenerator for the supplied auto-embedding index.
   *
   * @param lifecycleExecutor the Executor to use to run all miscellaneous work on
   * @param cursorManager the cursor manager that can be used for killing cursors when an index is
   *     dropped
   * @param initialSyncQueue the initial sync queue to enqueue resyncs onto
   * @param steadyStateManager the steady state manager used to create SteadyStateIndexManagers for
   *     the Index
   * @param indexGeneration the index whose lifecycle the created ReplicationIndexManager should own
   * @param documentIndexer the indexer used to process document events and commit the index
   * @param periodicCommitter the periodic committer, which triggers commits with a configured
   *     frequency
   * @return an ReplicationIndexManager that owns the supplied index
   */
  public static MaterializedViewGenerator create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      Duration requestRateLimitBackoffMs,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      boolean enableNaturalOrderScan,
      CompletableFuture<Void> preCondition) {
    return create(
        lifecycleExecutor,
        cursorManager,
        initialSyncQueue,
        steadyStateManager,
        indexGeneration,
        initializedIndex,
        documentIndexer,
        periodicCommitter,
        meterRegistry,
        featureFlags,
        DEFAULT_RESYNC_BACKOFF,
        DEFAULT_TRANSIENT_BACKOFF,
        requestRateLimitBackoffMs,
        enableNaturalOrderScan,
        preCondition);
  }

  @VisibleForTesting
  static MaterializedViewGenerator create(
      Executor lifecycleExecutor,
      MongotCursorManager cursorManager,
      InitialSyncQueue initialSyncQueue,
      SteadyStateManager steadyStateManager,
      IndexGeneration indexGeneration,
      InitializedIndex initializedIndex,
      DocumentIndexer documentIndexer,
      PeriodicIndexCommitter periodicCommitter,
      MeterRegistry meterRegistry,
      FeatureFlags featureFlags,
      Duration resyncBackoff,
      Duration transientBackoff,
      Duration requestRateLimitBackoffMs,
      boolean enableNaturalScan,
      CompletableFuture<Void> preCondition) {

    MaterializedViewGenerator manager =
        new MaterializedViewGenerator(
            lifecycleExecutor,
            cursorManager,
            initialSyncQueue,
            steadyStateManager,
            indexGeneration,
            initializedIndex,
            documentIndexer,
            periodicCommitter,
            new MetricsFactory("materializedViewGenerator", meterRegistry),
            featureFlags,
            resyncBackoff,
            transientBackoff,
            requestRateLimitBackoffMs,
            enableNaturalScan);

    // Schedule the manager to be initialized on the lifecycleExecutor, and fail the Index
    // if initialization fails.
    synchronized (manager) {
      manager.initFuture =
          preCondition.thenCompose(
              ignored ->
                  CompletableFuture.runAsync(manager::init, lifecycleExecutor)
                      .handleAsync(
                          (ignored1, throwable) -> {
                            if (throwable != null) {
                              if (featureFlags.isEnabled(
                                  Feature.RETAIN_FAILED_INDEX_DATA_ON_DISK)) {
                                manager.failAndCloseIndex(
                                    throwable, IndexStatus.Reason.INITIALIZATION_FAILED);
                              } else {
                                manager.failAndDropIndex(
                                    throwable, IndexStatus.Reason.INITIALIZATION_FAILED);
                              }
                            }

                            return null;
                          },
                          lifecycleExecutor));
    }

    return manager;
  }

  public MaterializedViewIndexGeneration getIndexGeneration() {
    return (MaterializedViewIndexGeneration) this.indexGeneration;
  }

  @Override
  // For auto-embedding index, we always resync instead of leaving the index in
  // RECOVERING_NON_TRANSIENT state.
  protected void handleSteadyStateNonInvalidatingResync(SteadyStateException steadyStateException) {
    this.logger.info(
        "Exception requiring resync occurred during steady state replication.",
        steadyStateException);
    enqueueInitialSync(IndexStatus.initialSync());
  }
}
