package com.xgen.mongot.replication.mongodb.common;

import static com.xgen.mongot.embedding.VectorOrError.EMPTY_INPUT_ERROR;
import static com.xgen.mongot.embedding.providers.configs.EmbeddingServiceConfig.ServiceTier;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildAutoEmbeddingDocumentEvent;
import static com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils.buildMaterializedViewDocumentEvent;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.flogger.FluentLogger;
import com.xgen.mongot.embedding.EmbeddingRequestContext;
import com.xgen.mongot.embedding.VectorOrError;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderNonTransientException;
import com.xgen.mongot.embedding.exceptions.EmbeddingProviderTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.embedding.providers.EmbeddingServiceManager;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelCatalog;
import com.xgen.mongot.embedding.providers.configs.EmbeddingModelConfig;
import com.xgen.mongot.embedding.utils.AutoEmbeddingDocumentUtils;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldDefinition;
import com.xgen.mongot.index.definition.VectorIndexFieldMapping;
import com.xgen.mongot.replication.mongodb.common.SchedulerQueue.Priority;
import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.FieldPath;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.bson.Vector;
import com.xgen.mongot.util.concurrent.NamedExecutorService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;

/**
 * The embedding indexing work scheduler accepts embedding and indexing work and schedules it to be
 * completed on an executor. Calls the embedding provider to generate embeddings for each batch and
 * indexes the document with its fields embedded.
 *
 * <p>This scheduler should only be used for indexes that use auto-embedding (have vector text
 * embedding fields).
 */
final class EmbeddingIndexingWorkScheduler extends IndexingWorkScheduler {

  private static final FluentLogger FLOGGER = FluentLogger.forEnclosingClass();

  // Max number of auto embed documents for an indexing bundle within the same
  // IndexingSchedulerBatch, greater number means more vectors to hold in memory before indexing
  private static final int MAX_AUTO_EMBED_DOCUMENT_BUNDLE_SIZE = 1000;

  private final Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier;

  private final boolean isMaterializedViewIndex;

  EmbeddingIndexingWorkScheduler(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier,
      boolean isMaterializedViewIndex) {
    super(indexingExecutor, "EmbeddingIndexingWorkScheduler");
    this.embeddingServiceManagerSupplier = embeddingServiceManagerSupplier;
    this.isMaterializedViewIndex = isMaterializedViewIndex;
  }

  /**
   * Creates and starts a new EmbeddingIndexingWorkScheduler.
   *
   * @return an EmbeddingIndexingWorkScheduler.
   */
  public static EmbeddingIndexingWorkScheduler create(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    EmbeddingIndexingWorkScheduler scheduler =
        new EmbeddingIndexingWorkScheduler(
            indexingExecutor, embeddingServiceManagerSupplier, false);
    scheduler.start();
    return scheduler;
  }

  /**
   * Creates and starts a new EmbeddingIndexingWorkScheduler for an auto-embedding materialized view
   * index.
   *
   * @return an EmbeddingIndexingWorkScheduler.
   */
  public static EmbeddingIndexingWorkScheduler createForMaterializedViewIndex(
      NamedExecutorService indexingExecutor,
      Supplier<EmbeddingServiceManager> embeddingServiceManagerSupplier) {
    EmbeddingIndexingWorkScheduler scheduler =
        new EmbeddingIndexingWorkScheduler(indexingExecutor, embeddingServiceManagerSupplier, true);
    scheduler.start();
    return scheduler;
  }

  @Override
  CompletableFuture<Void> getBatchTasksFuture(IndexingSchedulerBatch batch) {
    // Replace text fields in event documents with embedded vectors for VectorText indexes.
    IndexDefinition indexDefinition = batch.indexer.getIndexDefinition();

    String modelName =
        indexDefinition.asVectorDefinition().getFields().stream()
            .filter(
                e ->
                    e.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED
                        || e.getType() == VectorIndexFieldDefinition.Type.TEXT)
            .map(
                field -> {
                  if (field.getType() == VectorIndexFieldDefinition.Type.AUTO_EMBED) {
                    return field.asVectorAutoEmbedField().specification().modelName();
                  } else {
                    return field.asVectorTextField().specification().modelName();
                  }
                })
            .findFirst()
            .orElse(EmbeddingModelConfig.DEFAULT_EMBEDDING_MODEL_CONFIG.name());
    // If we have an unregistered modelName, we fail the request
    if (!EmbeddingModelCatalog.isModelRegistered(modelName.toLowerCase())) {
      return CompletableFuture.failedFuture(
          new EmbeddingProviderNonTransientException(
              String.format(
                  "CanonicalModel: %s not registered yet, supported models are: [%s]",
                  modelName, String.join(", ", EmbeddingModelCatalog.getAllSupportedModels()))));
    }
    EmbeddingModelConfig registeredModelConfig = EmbeddingModelCatalog.getModelConfig(modelName);
    List<CompletableFuture<List<DocumentEvent>>> indexingBundles =
        embed(
            batch.events,
            indexDefinition.asVectorDefinition().getMappings(),
            batch.priority,
            registeredModelConfig,
            indexDefinition);

    return FutureUtils.allOf(
        indexingBundles.stream()
            // Convert each indexing bundle to multiple indexing tasks.
            .map(
                bundleFuture ->
                    bundleFuture.thenComposeAsync(
                        eventBundleList ->
                            FutureUtils.allOf(
                                eventBundleList.stream()
                                    .map((doc) -> new IndexingTask(batch.indexer, doc))
                                    .map(
                                        (task) ->
                                            FutureUtils.checkedRunAsync(
                                                task,
                                                this.executor,
                                                FieldExceededLimitsException.class))
                                    .collect(Collectors.toList())),
                        this.executor))
            .collect(Collectors.toList()));
  }

  @Override
  void handleBatchException(IndexingSchedulerBatch batch, Throwable throwable) {
    if (throwable.getCause() instanceof EmbeddingProviderNonTransientException ex) {
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createFailed(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createNonInvalidatingResync(ex));
      }
    } else if (throwable.getCause() instanceof EmbeddingProviderTransientException ex) {
      // TODO(CLOUDP-305372): Find a way to skip already indexed document event in retries.
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createResumableTransient(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createTransient(ex));
      }
    } else if (throwable.getCause() instanceof MaterializedViewTransientException ex) {
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createResumableTransient(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createTransient(ex));
      }
    } else if (throwable.getCause() instanceof MaterializedViewNonTransientException ex) {
      if (batch.priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
          || batch.priority == Priority.INITIAL_SYNC_CHANGE_STREAM) {
        batch.future.completeExceptionally(InitialSyncException.createFailed(ex));
      } else {
        batch.future.completeExceptionally(SteadyStateException.createNonInvalidatingResync(ex));
      }
    } else {
      batch.future.completeExceptionally(throwable);
    }
  }

  @Override
  protected boolean shouldCommitOnFinalize() {
    return this.isMaterializedViewIndex;
  }

  /**
   * Splits events in this batch into multiple indexing bundles and replaces the given events with
   * events that contain the embedded vectors by calling external embedding service.
   *
   * <p>This modifies the List!
   */
  private List<CompletableFuture<List<DocumentEvent>>> embed(
      List<DocumentEvent> allEventsInBatch,
      VectorIndexFieldMapping fieldMapping,
      SchedulerQueue.Priority priority,
      EmbeddingModelConfig modelConfig,
      IndexDefinition indexDefinition) {
    List<Pair<List<DocumentEvent>, Set<String>>> embedBundles =
        getTextValueBundles(allEventsInBatch, fieldMapping, MAX_AUTO_EMBED_DOCUMENT_BUNDLE_SIZE);
    List<CompletableFuture<List<DocumentEvent>>> resultFutures = new ArrayList<>();
    for (Pair<List<DocumentEvent>, Set<String>> bundle : embedBundles) {
      // Needs to create a shallow copy for List<DocumentEvent> to avoid original list to be
      // referenced by CompletableFuture::UniApply even after completing futures, which may cause
      // memory leak.
      var events = new ArrayList<>(bundle.getLeft());
      CompletableFuture<Map<String, Vector>> embeddings =
          getEmbeddings(bundle.getRight(), priority, modelConfig, indexDefinition);
      resultFutures.add(
          // Change executor to use indexing executor here for better chaining with indexer.
          embeddings.thenApplyAsync(
              embeddingMap -> {
                // TODO(CLOUDP-332187): Supports per-FieldPath embedding processing for multi-model
                // indexes.
                var embeddingMapPerField =
                    fieldMapping.fieldMap().keySet().stream()
                        .collect(
                            ImmutableMap.toImmutableMap(
                                Function.identity(), ignored -> embeddingMap));
                for (int i = 0; i < events.size(); i++) {
                  DocumentEvent event = events.get(i);
                  if (!containsValidDocument(event)) {
                    continue;
                  }

                  // For filter-only updates, skip embedding transformation and pass through
                  // unchanged. The event already has filterFieldUpdates set, which
                  // MaterializedViewWriter will use for partial update.
                  if (event.getFilterFieldUpdates().isPresent()) {
                    continue;
                  }

                  try {
                    DocumentEvent autoEmbeddingDocumentEvent;
                    if (this.isMaterializedViewIndex) {
                      autoEmbeddingDocumentEvent =
                          buildMaterializedViewDocumentEvent(event, fieldMapping, embeddingMap);
                    } else {
                      autoEmbeddingDocumentEvent =
                          buildAutoEmbeddingDocumentEvent(
                              event, fieldMapping, embeddingMapPerField);
                    }
                    events.set(i, autoEmbeddingDocumentEvent);
                  } catch (IOException e) {
                    FLOGGER.atSevere().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
                        "Failed to replace string field values");
                  }
                }
                return events;
              },
              this.executor));
    }
    return resultFutures;
  }

  /**
   * Splits Documents events by bundle size, deletion and non-autoembedding(no matching text) events
   * are not limited by bundle size.
   */
  static List<Pair<List<DocumentEvent>, Set<String>>> getTextValueBundles(
      List<DocumentEvent> events, VectorIndexFieldMapping fieldMapping, int maxDocumentBundleSize) {
    // Step 1: Gets all (document, autoEmbeddingTextSet) pairs
    List<Pair<DocumentEvent, Set<String>>> documentTextPairs =
        events.stream()
            .map(
                event -> {
                  // Skip text extraction for filter-only updates - they don't need embeddings
                  if (containsValidDocument(event)
                      && event.getFilterFieldUpdates().isEmpty()) {
                    Set<String> stringValues = new HashSet<>();
                    try {
                      var autoEmbeddingTextPathMap =
                          AutoEmbeddingDocumentUtils.getVectorTextPathMap(
                              event.getDocument().get(), fieldMapping);
                      for (var entry : autoEmbeddingTextPathMap.entrySet()) {
                        FieldPath fieldPath = entry.getKey();
                        Set<String> textsInField = entry.getValue();

                        // Get reusable embeddings for this field
                        var reusableForField =
                            event.getAutoEmbeddings().getOrDefault(fieldPath, ImmutableMap.of());

                        // Only add texts that don't have reusable embeddings
                        for (String text : textsInField) {
                          if (!reusableForField.containsKey(text)) {
                            stringValues.add(text);
                          }
                        }
                      }
                    } catch (IOException e) {
                      FLOGGER.atSevere().atMostEvery(1, TimeUnit.MINUTES).withCause(e).log(
                          "Failed to get string values");
                    }
                    return Pair.of(event, stringValues);
                  } else {
                    return Pair.of(event, Set.<String>of());
                  }
                })
            .toList();

    // TODO(CLOUDP-331321): Move batching logic into embedding service manager
    // Step 2: For all pair with non empty autoEmbedding text set, partition them by max document
    // bundle size, and aggregate their autoEmbedding text set into one set per partition
    List<Pair<List<DocumentEvent>, Set<String>>> documentEventBundles =
        Lists.partition(
                documentTextPairs.stream().filter(pair -> !pair.getRight().isEmpty()).toList(),
                maxDocumentBundleSize)
            .stream()
            .map(
                eventBundle ->
                    Pair.of(
                        eventBundle.stream().map(Pair::getLeft).toList(),
                        eventBundle.stream()
                            .flatMap(eventPair -> eventPair.getRight().stream())
                            .collect(Collectors.toSet())))
            .collect(Collectors.toList());

    // Step 3: Append all no-op document events without any auto embedding text.
    List<DocumentEvent> noAutoEmbeddingEvents =
        documentTextPairs.stream()
            .filter(eventPair -> eventPair.getRight().isEmpty())
            .map(Pair::getLeft)
            .toList();
    if (!noAutoEmbeddingEvents.isEmpty()) {
      documentEventBundles.add(Pair.of(noAutoEmbeddingEvents, Set.of()));
    }
    return documentEventBundles;
  }

  private CompletableFuture<Map<String, Vector>> getEmbeddings(
      Set<String> stringsToEmbed,
      Priority priority,
      EmbeddingModelConfig modelConfig,
      IndexDefinition indexDefinition) {
    if (stringsToEmbed.isEmpty()) {
      return CompletableFuture.completedFuture(Map.of());
    }

    // Create EmbeddingRequestContext from IndexDefinition
    EmbeddingRequestContext context =
        new EmbeddingRequestContext(
            indexDefinition.getDatabase(),
            indexDefinition.getIndexId(),
            indexDefinition.getCollectionUuid());

    EmbeddingServiceManager serviceManager = this.embeddingServiceManagerSupplier.get();
    List<String> orderedStrings = new ArrayList<>(stringsToEmbed);

    ServiceTier tier =
        priority == Priority.INITIAL_SYNC_COLLECTION_SCAN
            ? ServiceTier.COLLECTION_SCAN
            : ServiceTier.CHANGE_STREAM;
    return serviceManager
        .embedAsync(orderedStrings, modelConfig, tier, context)
        .thenApply(
            embeddingList -> {
              Map<String, Vector> embeddings = new HashMap<>();
              Check.checkState(
                  embeddingList.size() == orderedStrings.size(),
                  "Result vectors size doesn't match input text size");
              for (int i = 0; i < embeddingList.size(); i++) {
                VectorOrError result = embeddingList.get(i);
                if (result.vector.isPresent()) {
                  embeddings.put(orderedStrings.get(i), result.vector.get());
                } else if (result != EMPTY_INPUT_ERROR && result.errorMessage.isPresent()) {
                  FLOGGER.atWarning().atMostEvery(1, TimeUnit.MINUTES).log(
                      "No embedding for %s due to error: %s",
                      orderedStrings.get(i), result.errorMessage.get());
                }
              }
              return embeddings;
            });
  }

  private static boolean containsValidDocument(DocumentEvent event) {
    return event.getEventType() != DocumentEvent.EventType.DELETE
        && event.getDocument().isPresent();
  }
}
