package com.xgen.mongot.replication.mongodb.synonyms;

import com.google.errorprone.annotations.Var;
import com.mongodb.MongoException;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.CollectionScanMongoClient;
import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.mongodb.CollectionScanFindCommand;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

/**
 * Responsible for scanning a synonym source collection and passing documents to a {@link
 * SynonymDocumentIndexer}.
 */
class SynonymCollectionScanner {
  private final SynonymSyncMongoClient mongoClient;
  private final SynonymCollectionScanRequest syncRequest;
  private Supplier<Optional<BsonTimestamp>> operationTimeSupplier;
  private Optional<BsonTimestamp> operationTime;
  private final FutureWrapper futureWrapper;
  private volatile boolean shutdown;
  protected final DefaultKeyValueLogger logger;

  @FunctionalInterface
  private interface FutureWrapper {
    void getOrWrapThrowable(CompletableFuture<Void> future) throws SynonymSyncException;
  }

  @FunctionalInterface
  interface Factory {
    SynonymCollectionScanner create(
        SynonymSyncMongoClient mongoClient, SynonymCollectionScanRequest syncRequest);
  }

  SynonymCollectionScanner(
      SynonymSyncMongoClient mongoClient, SynonymCollectionScanRequest syncRequest) {
    this.mongoClient = mongoClient;
    this.syncRequest = syncRequest;
    this.operationTime = Optional.empty();
    this.operationTimeSupplier = Optional::empty;
    this.futureWrapper = SynonymSyncException::getOrWrapThrowable;

    var synonymMappingId = this.syncRequest.getMappingId();
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", synonymMappingId.indexGenerationId.indexId);
    defaultKeyValues.put("generationId", synonymMappingId.indexGenerationId);
    defaultKeyValues.put("synonymMappingName", synonymMappingId.name);
    this.logger = DefaultKeyValueLogger.getLogger(SynonymCollectionScanner.class, defaultKeyValues);
  }

  /**
   * Scans the collection and indexes the documents into the Index. Returns an opTime that is
   * greater than or equal to the opTime of the final getMore.
   */
  public BsonTimestamp scan() throws SynonymSyncException {
    this.logger.info("Beginning collection scan.");

    CollectionScanMongoClient<SynonymSyncException> mongoClient = getClient();
    try (mongoClient) {

      // use this to synchronize on prior indexing, and buffer a batch to be indexed after.
      // to begin, there is no indexing to wait on, so set this future to complete.
      @Var CompletableFuture<Void> indexingFuture = CompletableFuture.completedFuture(null);

      while (mongoClient.hasNext()) {
        if (this.shutdown) {
          handleShutdown();
        }

        indexingFuture = scheduleNextWhenCompletes(indexingFuture, mongoClient);
      }

      // wait for the last indexing to complete
      this.futureWrapper.getOrWrapThrowable(indexingFuture);

      this.logger.info("Completed collection scan.");
      return getScanReturnValue();
    } catch (Exception e) {
      // on shutdown signal, processing is cancelled and a shutdown exception is thrown,
      // but we also need to cancel it in case of unexpected error during processing.
      boolean isShutdownException =
          e instanceof InitialSyncException && ((InitialSyncException) e).isShutdown();
      if (!isShutdownException) {
        FutureUtils.getAndSwallow(
            cancelProcessing(e),
            error -> this.logger.error("Failure during indexing cancellation", error));
      }

      throw e;
    }
  }

  /**
   * Inform this CollectionScanner to shut down if it's being run on a different thread.
   *
   * <p>Note that signalShutdown() does not guarantee that this CollectionScanner will have stopped
   * when it returns.
   */
  public void signalShutdown() {
    this.logger.info("Signalling CollectionScanner shutdown.");

    this.shutdown = true;
  }

  CollectionScanMongoClient<SynonymSyncException> getClient() throws SynonymSyncException {
    CollectionScanFindCommand findCommand =
        SynonymSyncMongoClient.collectionScanFindCommand(
            this.syncRequest.getSynonymMappingDefinition());
    CollectionScanMongoClient<SynonymSyncException> client =
        this.mongoClient.getFindCommandClient(findCommand, this.syncRequest.getNamespace());

    this.operationTimeSupplier =
        () -> {
          try {
            return Optional.of(client.getOperationTime());
          } catch (SynonymSyncException e) {
            this.logger.warn("could not get operation time from client");
            return Optional.empty();
          }
        };
    return client;
  }

  void handleShutdown() throws SynonymSyncException {
    this.logger.info("Shutting down the SynonymCollectionScanner.");
    this.cancelProcessing(SynonymSyncException.createShutDown());
    throw SynonymSyncException.createShutDown();
  }

  CompletableFuture<Void> cancelProcessing(Throwable reason) {
    // Populate the operation time on cancel, if it is present.
    this.operationTime = this.operationTimeSupplier.get();
    return FutureUtils.COMPLETED_FUTURE;
  }

  CompletableFuture<Void> scheduleWork(List<RawBsonDocument> batch) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    try {
      future.complete(this.syncRequest.getDocumentIndexer().indexDocumentBatch(batch));
    } catch (SynonymSyncException e) {
      cancelProcessing(e);
      future.completeExceptionally(e);
    }
    return future;
  }

  BsonTimestamp getScanReturnValue() throws SynonymSyncException {
    this.operationTime = this.operationTimeSupplier.get();
    if (this.operationTime.isEmpty()) {
      throw SynonymSyncException.createTransient(
          new MongoException("scan did not contain operationTime for unknown reason"));
    }
    return this.operationTime.get();
  }

  /**
   * Gets the operationTime when this collectionScan began. This value is populated when a
   * collection scan is completed or cancelled.
   */
  Optional<BsonTimestamp> getOperationTime() {
    return this.operationTime;
  }

  /**
   * Buffers the next batch, then waits for the indexingFuture to complete, schedules the batch, and
   * returns the batch's indexing future. If we fail to buffer the next batch, we wait until the
   * indexing future completes to throw the exception.
   */
  private CompletableFuture<Void> scheduleNextWhenCompletes(
      CompletableFuture<Void> indexingFuture,
      CollectionScanMongoClient<SynonymSyncException> mongoClient)
      throws SynonymSyncException {
    List<RawBsonDocument> batch;
    try {
      batch = mongoClient.getNext();
    } catch (Exception ex) {
      // wait for the last indexing to complete before rethrowing
      this.futureWrapper.getOrWrapThrowable(indexingFuture);
      throw ex;
    }

    // wait for indexingFuture to complete, then schedule the next batch
    return scheduleBatchWhenCompletes(indexingFuture, batch);
  }

  /**
   * Wait for indexingFuture to complete, then schedule the given batch and return it's indexing
   * future.
   */
  private CompletableFuture<Void> scheduleBatchWhenCompletes(
      CompletableFuture<Void> indexingFuture, List<RawBsonDocument> batch)
      throws SynonymSyncException {

    // wait for the previous indexing to complete
    this.futureWrapper.getOrWrapThrowable(indexingFuture);
    // now that the previous indexing has finished, we can schedule this batch
    return scheduleWork(batch);
  }
}
