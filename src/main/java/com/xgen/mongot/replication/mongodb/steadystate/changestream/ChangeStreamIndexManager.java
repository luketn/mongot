package com.xgen.mongot.replication.mongodb.steadystate.changestream;

import static com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.bsonDocumentToChangeStreamDocument;
import static com.xgen.mongot.util.Check.checkState;

import com.google.common.base.Stopwatch;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.xgen.mongot.index.DocsExceededLimitsException;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.ChangeStreamEventCheckException;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamDocumentUtils.DocumentEventBatch;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.ChangeStreams;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.IndexCommitUserData;
import com.xgen.mongot.replication.mongodb.common.IndexingWorkScheduler;
import com.xgen.mongot.replication.mongodb.common.ResumeTokenUtils;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.replication.mongodb.common.SteadyStateException;
import com.xgen.mongot.util.Crash;
import com.xgen.mongot.util.FutureUtils;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import org.apache.commons.codec.DecoderException;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.types.ObjectId;

/** One instance per generation id. */
class ChangeStreamIndexManager {

  /**
   * Encapsulates the results of an indexBatch call. Holds a future representing when the indexing
   * finishes. Also holds whether or not the batch had a lifecycle event.
   */
  static final class BatchInfo {
    final CompletableFuture<Void> indexingFuture;
    final Optional<SteadyStateException> lifecycleEvent;

    BatchInfo(
        CompletableFuture<Void> indexingFuture, Optional<SteadyStateException> lifecycleEvent) {

      this.indexingFuture = indexingFuture;
      this.lifecycleEvent = lifecycleEvent;
    }

    BatchInfo(CompletableFuture<Void> indexingFuture) {
      this(indexingFuture, Optional.empty());
    }
  }

  /**
   * Encapsulates the results of a handleLifecycleEvents call. Holds a list of {@link
   * ChangeStreamDocument} Also holds whether or not the batch had a lifecycle event.
   */
  private record ProcessedBatch(
      List<ChangeStreamDocument<RawBsonDocument>> batch,
      Optional<SteadyStateException> lifecycleEvent) {}

  protected final DefaultKeyValueLogger logger;
  protected final IndexDefinition indexDefinition;
  protected final IndexingWorkScheduler indexingWorkScheduler;
  protected final DocumentIndexer documentIndexer;
  protected final GenerationId generationId;
  protected final ObjectId attemptId;
  protected final MongoNamespace namespace;
  protected final IndexMetricsUpdater indexMetricsUpdater;
  private final Consumer<ChangeStreamResumeInfo> resumeInfoUpdater;

  // use these to limit how often we write the optime to the log
  private volatile Instant optimeLastUpdate;
  private static final Duration OPTIME_LOG_PERIOD = Duration.ofMinutes(1);

  private static final Set<OperationType> INVALIDATING_OPERATIONS =
      Set.of(
          OperationType.RENAME,
          OperationType.DROP,
          OperationType.DROP_DATABASE,
          OperationType.INVALIDATE);

  protected Optional<MongoNamespace> renameNamespace;

  /**
   * lifecycleFuture is the future that should be completed when an exception occurs while indexing
   * or shutdown() is invoked.
   *
   * <p>lifecycleFuture thus can be used by higher-level components to monitor the health of the
   * ChangeStreamIndexManager and react to events.
   */
  protected final CompletableFuture<Void> lifecycleFuture;

  /** indexingFuture is a future that represents asynchronous work being done to index a batch. */
  @GuardedBy("this")
  protected CompletableFuture<Void> latestIndexingFuture;

  @GuardedBy("this")
  private boolean shutdown;

  protected ChangeStreamIndexManager(
      IndexDefinition indexDefinition,
      DefaultKeyValueLogger logger,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer documentIndexer,
      MongoNamespace namespace,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      CompletableFuture<Void> lifecycleFuture,
      GenerationId generationId) {

    this.logger = logger;
    this.indexDefinition = indexDefinition;
    this.indexingWorkScheduler = indexingWorkScheduler;
    this.documentIndexer = documentIndexer;
    this.namespace = namespace;
    this.resumeInfoUpdater = resumeInfoUpdater;
    this.indexMetricsUpdater = indexMetricsUpdater;
    this.lifecycleFuture = lifecycleFuture;
    this.generationId = generationId;

    this.latestIndexingFuture = FutureUtils.COMPLETED_FUTURE;
    this.shutdown = false;
    this.optimeLastUpdate = Instant.MIN;
    this.renameNamespace = Optional.empty();
    this.attemptId = new ObjectId();
  }

  static ChangeStreamIndexManager createDefault(
      IndexDefinition indexDefinition,
      IndexingWorkScheduler indexingWorkScheduler,
      DocumentIndexer documentIndexer,
      MongoNamespace namespace,
      Consumer<ChangeStreamResumeInfo> resumeInfoUpdater,
      IndexMetricsUpdater indexMetricsUpdater,
      CompletableFuture<Void> lifecycleFuture,
      GenerationId generationId) {

    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", generationId.indexId);
    defaultKeyValues.put("generationId", generationId);
    var logger = DefaultKeyValueLogger.getLogger(ChangeStreamIndexManager.class, defaultKeyValues);

    logger.info("Created default manager for index");

    return new ChangeStreamIndexManager(
        indexDefinition,
        logger,
        indexingWorkScheduler,
        documentIndexer,
        namespace,
        resumeInfoUpdater,
        indexMetricsUpdater,
        lifecycleFuture,
        generationId);
  }

  /**
   * Gracefully shuts down the change stream indexing.
   *
   * @return a future that completes when the scheduled work for the index has completed. The future
   *     will only ever complete successfully.
   */
  public synchronized CompletableFuture<Void> shutdown() {
    // Flipping this flag should prevent future work from being scheduled.
    this.shutdown = true;

    // Complete the external future with a SHUT_DOWN SteadyStateException when the current indexing
    // task completes no matter how it completes.
    Crash.because("failed indexing batch")
        .ifCompletesExceptionally(
            FutureUtils.swallowedFuture(this.latestIndexingFuture)
                .handle(
                    (result, throwable) -> {
                      if (!this.lifecycleFuture.isDone()) {
                        failLifecycle(SteadyStateException.createShutDown());
                      }
                      return null;
                    }));

    // Return a future that returns when the external future is completed, but doesn't complete
    // exceptionally.
    return FutureUtils.swallowedFuture(this.lifecycleFuture);
  }

  public synchronized boolean isShutdown() {
    return this.shutdown;
  }

  synchronized BatchInfo indexBatch(
      ChangeStreamBatch batch,
      DocumentMetricsUpdater metricsUpdater,
      Timer preprocessingBatchTimer) {
    // If we're given a batch after we've been shut down, return a failed future with a SHUT_DOWN
    // SteadyStateException to signal that we've been shut down.
    if (this.shutdown) {
      return new BatchInfo(CompletableFuture.failedFuture(SteadyStateException.createShutDown()));
    }

    // Otherwise the external future should not have completed yet.
    if (this.lifecycleFuture.isDone()) {
      return new BatchInfo(this.lifecycleFuture);
    }

    var timer = Stopwatch.createStarted();
    // Schedule the batch to be indexed, and update the resumeInfo only after the batch has been
    // indexed.
    Optional<ChangeStreamResumeInfo> resumeInfo = getResumeInfo(batch);

    // Preprocess the batch to find if there are any lifecycle events and get a final list of
    // changes to schedule for indexing.
    var rawEvents = batch.getRawEvents();
    ProcessedBatch processedBatch =
        handleLifecycleEvents(
            ChangeStreamDocumentUtils.asLazyDecodableChangeStreamDocuments(rawEvents));
    preprocessingBatchTimer.record(timer.stop().elapsed());

    ChangeStreamDocumentUtils.recordChangeStreamEventSizes(
        rawEvents, this.indexMetricsUpdater.getIndexingMetricsUpdater()::recordDocumentSizeBytes);
    // Should not throw, as all lifecycle events have been removed from the batch
    ChangeStreamDocumentUtils.DocumentEventBatch documentEventBatch =
        ChangeStreamDocumentUtils.handleDocumentEvents(
            // Inapplicable updates are already filtered out in handleLifecycleEvents.
            processedBatch.batch,
            this.indexDefinition,
            this.indexDefinition.createFieldDefinitionResolver(
                this.generationId.generation.indexFormatVersion),
            false /* areUpdateEventsPrefiltered */);

    metricsUpdater.accept(
        documentEventBatch.updatesWitnessed,
        documentEventBatch.updatesApplicable,
        documentEventBatch.skippedDocumentsWithoutMetadataNamespace);

    List<DocumentEvent> finalChangeEvents = documentEventBatch.finalChangeEvents;

    CompletableFuture<Void> indexingFuture = new CompletableFuture<>();

    CompletableFuture<Void> schedulingFuture =
        this.indexingWorkScheduler
            .schedule(
                finalChangeEvents,
                Priority.STEADY_STATE_CHANGE_STREAM,
                this.documentIndexer,
                this.generationId,
                Optional.of(this.attemptId),
                resumeInfo.map(
                    info ->
                        IndexCommitUserData.createChangeStreamResume(
                            info, this.generationId.generation.indexFormatVersion)),
                this.indexMetricsUpdater.getIndexingMetricsUpdater())
            .thenRun(() -> recordPerBatchMetrics(documentEventBatch));

    this.latestIndexingFuture =
        postIndexingPipeline(
            schedulingFuture,
            indexingFuture,
            resumeInfo,
            processedBatch.lifecycleEvent,
            batch.getCommandOperationTime());

    return new BatchInfo(indexingFuture, processedBatch.lifecycleEvent);
  }

  protected CompletableFuture<Void> postIndexingPipeline(
      CompletableFuture<Void> schedulingFuture,
      CompletableFuture<Void> indexingFuture,
      Optional<ChangeStreamResumeInfo> resumeInfo,
      Optional<SteadyStateException> lifecycleEvent,
      BsonTimestamp commandOperationTime) {
    return schedulingFuture
        .thenRun(
            () -> {
              resumeInfo.ifPresent(this.resumeInfoUpdater);
              if (resumeInfo.isPresent()) {
                try {
                  BsonTimestamp opTime =
                      ResumeTokenUtils.opTimeFromResumeToken(resumeInfo.get().getResumeToken());
                  this.indexMetricsUpdater
                      .getIndexingMetricsUpdater()
                      .getReplicationOpTimeInfo()
                      .update(opTime.getValue(), commandOperationTime.getValue());

                  if (Instant.now().isAfter(this.optimeLastUpdate.plus(OPTIME_LOG_PERIOD))) {
                    this.optimeLastUpdate = Instant.now();
                    this.logger.debug("Processed change stream batch with optime={}", opTime);
                  }
                } catch (DecoderException ex) {
                  this.logger.warn("Error reading optime from resume token.", ex);
                }
              }

              // Only way we get here is if ChangeStreamIndexManager::indexBatch
              // found a lifecycle event in the batch. In that case, it will
              // not be re-enqueued by the ChangeStreamManager, so we don't need to cancel
              // here.
              lifecycleEvent.ifPresent(this.lifecycleFuture::completeExceptionally);

              // Indexing completed successfully, even if there was a lifecycleEvent
              indexingFuture.complete(null);
            })
        .exceptionally(
            throwable -> {
              // Complete the indexing future with the failure. Do not complete the
              // lifecycleFuture. The ChangeStreamManager will see the indexing
              // failure and handle it.
              Throwable failure = unwrapIndexingException(throwable);

              indexingFuture.completeExceptionally(failure);

              return null;
            });
  }

  protected void recordPerBatchMetrics(DocumentEventBatch documentEventBatch) {
    if (documentEventBatch.applicableDocumentsTotal > 0) {
      var steadyStateMetrics =
          this.indexMetricsUpdater.getReplicationMetricsUpdater().getSteadyStateMetrics();

      steadyStateMetrics
          .getBatchTotalApplicableDocuments()
          .record(documentEventBatch.applicableDocumentsTotal);

      steadyStateMetrics
          .getBatchTotalApplicableBytes()
          .record(documentEventBatch.applicableDocumentsTotalBytes);
    }
  }

  /**
   * Cancels any work that is queued for this index and schedules completion of the lifecycle future
   * after the in-flight batches are processed.
   */
  synchronized void failLifecycle(Throwable reason) {
    this.indexingWorkScheduler
        .cancel(this.generationId, Optional.of(this.attemptId), reason)
        .handle(
            (result, throwable) -> {
              this.logger.warn(
                  "failing generation lifecycle after waiting for any in-flight "
                      + "batches to complete");
              return this.lifecycleFuture.completeExceptionally(reason);
            });
  }

  protected Optional<ChangeStreamResumeInfo> getResumeInfo(ChangeStreamBatch batch) {
    int numEvents = batch.getRawEvents().size();
    if (numEvents == 0) {
      return Optional.of(
          ChangeStreamResumeInfo.create(this.namespace, batch.getPostBatchResumeToken()));
    }

    // In general, we want to resume the changestream after the current batch. However, there are
    // two cases involving invalidating events. Invalidating events involve an event which causes
    // invalidation (i.e. DROP, DROP_DATABASE,or RENAME), as well as the actual INVALIDATE event.

    // First, it is possible for the DROP/DROP_DATABASE/RENAME event to be in a different batch than
    // the INVALIDATE event if the batch is already full.

    // If we were to commit the resumeToken associated with the DROP/DROP_DATABASE/RENAME event, we
    // would resume the change stream and immediately see an INVALIDATE event, without knowing if it
    // was due to a drop vs a rename.

    // Secondly, it is possible for mongot to crash after a RENAME event, but before the new
    // changestream has caused any index commits to occur. In this case, we want the index to see
    // the rename event after mongot restarts, so we want to commit resume info from before the
    // RENAME event.

    // To avoid these cases, we find the last non-invalidating (i.e. not DROP, DROP_DATABASE,
    // RENAME, or INVALIDATE) event, and use that as the resume info. In most cases, this resume
    // info will not be used again.

    OperationType lastEventType =
        bsonDocumentToChangeStreamDocument(batch.getRawEvents().get(numEvents - 1))
            .getOperationType();
    boolean lastEventInvalidates = INVALIDATING_OPERATIONS.contains(lastEventType);

    if (!lastEventInvalidates) {
      return Optional.of(
          ChangeStreamResumeInfo.create(this.namespace, batch.getPostBatchResumeToken()));
    }

    // Return resume info of last non-invalidating operation
    for (RawBsonDocument rawEvent : batch.getRawEvents().reversed()) {
      ChangeStreamDocument<RawBsonDocument> changeStreamDoc =
          bsonDocumentToChangeStreamDocument(rawEvent);
      if (!INVALIDATING_OPERATIONS.contains(changeStreamDoc.getOperationType())) {
        return Optional.of(
            ChangeStreamResumeInfo.create(this.namespace, changeStreamDoc.getResumeToken()));
      }
    }

    // All events are invalidating; do not change resume info
    return Optional.empty();
  }

  /**
   * Look through the batch and handle any lifecycleEvents that are found. If an exception needs to
   * be thrown to trigger lifecycle changes, store that exception. Return a list of all document
   * events (insert, update, delete, replace) that happen before the lifecycleEvent (or all, if no
   * lifecycleEvent).
   *
   * @return list of all document events (insert, update, delete, replace) that happen before the
   *     lifecycleEvent (all if no lifecycleEvent is found).
   */
  private ProcessedBatch handleLifecycleEvents(List<ChangeStreamDocument<RawBsonDocument>> batch) {

    List<ChangeStreamDocument<RawBsonDocument>> documentEvents = new ArrayList<>();
    for (ChangeStreamDocument<RawBsonDocument> event : batch) {

      // If the last event was a rename, this should be an INVALIDATE.
      if (this.renameNamespace.isPresent()) {
        return new ProcessedBatch(documentEvents, Optional.of(handleRenameEvent(event)));
      }

      switch (Objects.requireNonNull(event.getOperationType())) {
        case DROP:
        case DROP_DATABASE:
          logChangeStreamEventDetails(event);
          return new ProcessedBatch(
              documentEvents, Optional.of(SteadyStateException.createDropped()));

        case RENAME:
          logChangeStreamEventDetails(event);
          if (Objects.isNull(event.getDestinationNamespace())) {
            return new ProcessedBatch(
                documentEvents,
                Optional.of(
                    SteadyStateException.createNonInvalidatingResync(
                        new MongoChangeStreamException(
                            "RENAME event did not have destination namespace"))));
          }
          if (ChangeStreams.renameCausedCollectionDrop(event, this.namespace)) {
            this.logger.info(
                "rename event caused collection drop. Renamed: {} to: {} (current namespace: {})",
                event.getNamespace(),
                event.getDestinationNamespace(),
                this.namespace);

            return new ProcessedBatch(
                documentEvents,
                Optional.of(
                    SteadyStateException.createDropped(
                        String.format("collection overwritten by %s", event.getNamespace()))));
          }

          // collection was renamed but not dropped:
          MongoNamespace destinationNamespace =
              Objects.requireNonNull(
                  event.getDestinationNamespace(),
                  "RENAME event did not have a destination namespace");

          // If this event is a RENAME, flag that so that the next event (which should be an
          // INVALIDATE) can be handled.
          this.renameNamespace = Optional.of(destinationNamespace);
          break;
        case INVALIDATE:
          // If we saw an INVALIDATE event without a preceding RENAME event, get the resumeToken to
          // be used in startAfter.
          logChangeStreamEventDetails(event);
          return new ProcessedBatch(
              documentEvents,
              Optional.of(
                  SteadyStateException.createInvalidated(
                      ChangeStreamResumeInfo.create(this.namespace, event.getResumeToken()))));
        case OTHER:
          // If we saw an event we can't process, then there's no use in trying to restart
          // the change stream. Resync and hope for the best.
          logChangeStreamEventDetails(event);
          return new ProcessedBatch(
              documentEvents,
              Optional.of(
                  SteadyStateException.createRequiresResync(
                      String.format("witnessed unknown change stream event: %s", event))));

        case INSERT:
        case UPDATE:
        case REPLACE:
        case DELETE:
          documentEvents.add(event);
          break;
      }
    }

    return new ProcessedBatch(documentEvents, Optional.empty());
  }

  /** Handles the event after a RENAME by returning the lifeCycleException. */
  protected SteadyStateException handleRenameEvent(ChangeStreamDocument<RawBsonDocument> event) {
    checkState(this.renameNamespace.isPresent(), "renameNamespace must be present");

    // We expect that the event following a rename is an INVALIDATE event.
    OperationType eventOperationType = Objects.requireNonNull(event.getOperationType());
    if (!eventOperationType.equals(OperationType.INVALIDATE)) {
      return SteadyStateException.createRequiresResync(
          String.format(
              "witnessed event of type %s after RENAME, expected INVALIDATE", eventOperationType));
    }

    // Get the resumeToken from the INVALIDATE event so it can be used in startAfter.
    return SteadyStateException.createRenamed(
        ChangeStreamResumeInfo.create(this.renameNamespace.get(), event.getResumeToken()));
  }

  protected void logChangeStreamEventDetails(ChangeStreamDocument<RawBsonDocument> event) {
    this.logger.info(
        "Received a {} change-stream event for index."
            + " Details: [clusterTime:{}, resumeToken:{}, namespace:{}, "
            + "destinationNamespace:{}, database:{}, documentKey:{}]",
        event.getOperationType(),
        event.getClusterTime(),
        event.getResumeToken(),
        event.getNamespace(),
        event.getDestinationNamespaceDocument(),
        event.getDatabaseName(),
        event.getDocumentKey());
  }

  public GenerationId getGenerationId() {
    return this.generationId;
  }

  private static Throwable unwrapIndexingException(Throwable throwable) {
    Throwable unwrapped =
        throwable instanceof CompletionException && throwable.getCause() != null
            ? throwable.getCause()
            : throwable;

    if (unwrapped instanceof FieldExceededLimitsException f) {
      return SteadyStateException.createFieldExceeded(f.getMessage());
    }

    if (unwrapped instanceof ChangeStreamEventCheckException && unwrapped.getCause() != null) {
      return unwrapped.getCause();
    }

    if (unwrapped instanceof DocsExceededLimitsException) {
      return SteadyStateException.createDocsExceeded(unwrapped);
    }

    return unwrapped;
  }

  @FunctionalInterface
  public interface DocumentMetricsUpdater {

    void accept(
        int updatesWitnessed, int updatesApplicable, int skippedDocumentsWithoutMetadataNamespace);
  }
}
