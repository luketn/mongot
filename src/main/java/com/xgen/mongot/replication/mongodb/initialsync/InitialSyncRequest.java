package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.index.IndexMetricsUpdater;
import com.xgen.mongot.index.definition.IndexDefinitionGeneration;
import com.xgen.mongot.replication.mongodb.common.ChangeStreamResumeInfo;
import com.xgen.mongot.replication.mongodb.common.DocumentIndexer;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * InitialSyncRequest is a data class representing the information required about how to run and
 * handle the result of an initial sync.
 */
class InitialSyncRequest {

  /** The DocumentIndexer used to index documents during the initial sync. */
  private final DocumentIndexer documentIndexer;

  /** The definition of the Index that is being synced. */
  private final IndexDefinitionGeneration indexDefinitionGeneration;

  /**
   * A callback to run when the initial sync has been picked up off the queue and is about to be
   * run.
   */
  private final Runnable initialSyncStartingHandler;

  /** A future to complete when the initial sync completes. */
  private final CompletableFuture<ChangeStreamResumeInfo> onCompleteFuture;

  /** The optional initial sync resume info, indicates this initial sync request is a resume. */
  private final Optional<InitialSyncResumeInfo> resumeInfo;

  /** A metric updater for index metrics (e.g., indexing, initial sync, decoding). */
  private final IndexMetricsUpdater indexMetricsUpdater;

  /** A flag indicating if the natural order scan will be used. */
  private final boolean useNaturalOrderScan;

  /**
   * A flag indicating if the matchCollectionUuidForUpdateLookup change stream parameter should be
   * removed because it is unsupported.
   */
  private final boolean removeMatchCollectionUuid;

  InitialSyncRequest(
      DocumentIndexer documentIndexer,
      IndexDefinitionGeneration indexDefinitionGeneration,
      Runnable initialSyncStartingHandler,
      CompletableFuture<ChangeStreamResumeInfo> onCompleteFuture,
      Optional<InitialSyncResumeInfo> resumeInfo,
      IndexMetricsUpdater indexMetricsUpdater,
      boolean removeMatchCollectionUuid,
      boolean useNaturalOrderScan) {
    this.documentIndexer = documentIndexer;
    this.indexDefinitionGeneration = indexDefinitionGeneration;
    this.initialSyncStartingHandler = initialSyncStartingHandler;
    this.onCompleteFuture = onCompleteFuture;
    this.resumeInfo = resumeInfo;
    this.indexMetricsUpdater = indexMetricsUpdater;
    this.useNaturalOrderScan = useNaturalOrderScan;
    this.removeMatchCollectionUuid = removeMatchCollectionUuid;
  }

  DocumentIndexer getDocumentIndexer() {
    return this.documentIndexer;
  }

  IndexDefinitionGeneration getIndexDefinitionGeneration() {
    return this.indexDefinitionGeneration;
  }

  Runnable getInitialSyncStartingHandler() {
    return this.initialSyncStartingHandler;
  }

  CompletableFuture<ChangeStreamResumeInfo> getOnCompleteFuture() {

    return this.onCompleteFuture;
  }

  Optional<InitialSyncResumeInfo> getResumeInfo() {
    return this.resumeInfo;
  }

  public IndexMetricsUpdater getIndexMetricsUpdater() {
    return this.indexMetricsUpdater;
  }

  public boolean getUseNaturalOrderScan() {
    return this.useNaturalOrderScan;
  }

  boolean isRemoveMatchCollectionUuid() {
    return this.removeMatchCollectionUuid;
  }
}
