package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkSchedulerFactory.IndexingStrategy;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Indexing work scheduler for indexes that use a custom vector engine. Performs batch preparation
 * (e.g. custom vector ending ID resolution) before indexing individual documents in the batch.
 */
final class CustomVectorEngineIndexingWorkScheduler extends IndexingWorkScheduler {

  CustomVectorEngineIndexingWorkScheduler(NamedExecutorService executor) {
    super(executor, IndexingStrategy.CUSTOM_VECTOR_ENGINE);
  }

  /**
   * Creates and starts a new CustomVectorEngineIndexingWorkScheduler.
   *
   * @return a CustomVectorEngineIndexingWorkScheduler.
   */
  public static CustomVectorEngineIndexingWorkScheduler create(NamedExecutorService executor) {
    CustomVectorEngineIndexingWorkScheduler scheduler =
        new CustomVectorEngineIndexingWorkScheduler(executor);
    scheduler.start();
    return scheduler;
  }

  @Override
  CompletableFuture<Void> getBatchTasksFuture(IndexingSchedulerBatch batch) {
    Context parentContext = Context.current();
    return CompletableFuture.supplyAsync(
            () -> batch.indexer.prepareBatch(batch.events), this.executor)
        .thenCompose(
            preparedEvents ->
                FutureUtils.allOf(
                    preparedEvents.stream()
                        .map(event -> new IndexingTask(batch.indexer, event))
                        .map(
                            task ->
                                FutureUtils.checkedRunAsync(
                                    () -> {
                                      try (Scope ignored = parentContext.makeCurrent()) {
                                        task.run();
                                      }
                                    },
                                    this.executor,
                                    FieldExceededLimitsException.class))
                        .collect(Collectors.toList())));
  }

  @Override
  void handleBatchException(IndexingSchedulerBatch batch, Throwable throwable) {
    batch.future.completeExceptionally(throwable);
  }
}
