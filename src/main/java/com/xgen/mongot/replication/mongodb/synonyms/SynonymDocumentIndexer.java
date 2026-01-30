package com.xgen.mongot.replication.mongodb.synonyms;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.SynonymMappingDefinition;
import com.xgen.mongot.index.synonym.SynonymDocument;
import com.xgen.mongot.index.synonym.SynonymMapping;
import com.xgen.mongot.index.synonym.SynonymRegistry;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.index.version.SynonymMappingId;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.replication.mongodb.common.SynonymSyncException;
import com.xgen.mongot.util.CheckedStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Optional;
import org.bson.RawBsonDocument;

/**
 * {@link SynonymDocumentIndexer} is responsible for inserting {@link SynonymDocument}s into a
 * {@link SynonymMapping.Builder}, creating a {@link SynonymMapping}, and updating the supplied
 * {@link SynonymRegistry} with that artifact.
 *
 * <p>SynonymDocumentIndexer also keeps track of the number of synonym documents it indexes in
 * pursuit of enforcing limits.
 */
public class SynonymDocumentIndexer {
  private final DefaultKeyValueLogger logger;
  private final SynonymRegistry synonymRegistry;
  private final SynonymMappingDefinition synonymMappingDefinition;
  private final SynonymMapping.Builder builder;
  private final Optional<Limits> limits;

  static class Limits {
    final int maxDocsPerSynonymCollection;
    int count;

    Limits(int maxDocsPerSynonymCollection) {
      this.maxDocsPerSynonymCollection = maxDocsPerSynonymCollection;
      this.count = 0;
    }

    /**
     * Increment the count by the provided {@code batchSize}, and throws a {@link
     * SynonymSyncException} of type {@link SynonymSyncException.Type#FIELD_EXCEEDED} if this batch
     * makes limits exceeded.
     */
    void ensureWithin(int batchSize) throws SynonymSyncException {
      this.count += batchSize;

      Optional<FieldExceededLimitsException> maybeExceeded = exceededFieldLimit();
      if (maybeExceeded.isPresent()) {
        throw SynonymSyncException.createFieldExceeded(maybeExceeded.get());
      }
    }

    private Optional<FieldExceededLimitsException> exceededFieldLimit() {
      if (this.count > this.maxDocsPerSynonymCollection) {
        return Optional.of(
            new FieldExceededLimitsException(
                String.format(
                    "Synonym document field limit exceeded: %s > %s",
                    this.count, this.maxDocsPerSynonymCollection)));
      }

      return Optional.empty();
    }
  }

  @VisibleForTesting
  SynonymDocumentIndexer(
      SynonymRegistry synonymRegistry,
      SynonymMappingDefinition definition,
      Optional<Limits> limits) {
    this(
        DefaultKeyValueLogger.getLogger(SynonymDocumentIndexer.class, new HashMap<>()),
        synonymRegistry,
        definition,
        limits);
  }

  private SynonymDocumentIndexer(
      DefaultKeyValueLogger logger,
      SynonymRegistry synonymRegistry,
      SynonymMappingDefinition definition,
      Optional<Limits> limits) {
    this.logger = logger;
    this.synonymRegistry = synonymRegistry;
    this.synonymMappingDefinition = definition;
    this.builder = synonymRegistry.mappingBuilder(definition);
    this.limits = limits;
  }

  static SynonymDocumentIndexerFactory factory(GenerationId generationId) {
    return (synRegistry, synDefinition) -> {
      var synonymMappingId = SynonymMappingId.from(generationId, synDefinition.name());
      HashMap<String, Object> defaultKeyValues = new HashMap<>();
      defaultKeyValues.put("indexId", generationId.indexId);
      defaultKeyValues.put("generationId", generationId);
      defaultKeyValues.put("synonymMappingName", synonymMappingId.name);
      return new SynonymDocumentIndexer(
          DefaultKeyValueLogger.getLogger(SynonymDocumentIndexerFactory.class, defaultKeyValues),
          synRegistry,
          synDefinition,
          synRegistry.getMaxDocsPerSynonymCollection().map(Limits::new));
    };
  }

  /**
   * Index a collection of DocumentEvents. If configured with limits, increments the number of
   * indexed documents by the number of documents in this batch.
   *
   * @throws SynonymSyncException if it encounters an error while indexing (e.g. a malformed
   *     SynonymDocument).
   */
  Void indexDocumentBatch(Collection<RawBsonDocument> batch) throws SynonymSyncException {
    if (this.limits.isPresent()) {
      this.limits.get().ensureWithin(batch.size());
    }

    CheckedStream.from(batch).forEachChecked(this::indexDocumentEvent);
    return null;
  }

  /**
   * Attempt to parse a {@link DocumentEvent} into a {@link SynonymDocument}, and add synonyms from
   * that document to the SynonymMapping.Builder.
   *
   * @throws SynonymSyncException On malformed or invalid {@link SynonymDocument}. See {@link
   *     SynonymMapping.Builder#addDocument(SynonymDocument)}.
   */
  private void indexDocumentEvent(RawBsonDocument bson) throws SynonymSyncException {
    SynonymSyncException.wrapIfThrows(
        () -> this.builder.addDocument(SynonymDocument.fromBson(bson)));
  }

  /**
   * Build the {@link SynonymMapping} and update the {@link SynonymRegistry}.
   *
   * @throws SynonymSyncException If the synonym mapping throws an exception. See {@link
   *     SynonymMapping.Builder#build()}.
   */
  void complete() throws SynonymSyncException {
    this.logger.info("completed building synonym mapping");

    this.synonymRegistry.update(
        this.synonymMappingDefinition.name(),
        SynonymSyncException.wrapIfThrows(this.builder::build));
  }

  /**
   * Perform a final action for this indexing task, under exceptional conditions. Drops an invalid
   * mapping, and clears a mapping on collection drop.
   *
   * @param exception The exceptional event causing this document indexer to complete exceptionally
   *     instead of normally.
   */
  void completeExceptionally(SynonymSyncException exception) {
    this.logger.info(
        "synonym mapping completed exceptionally with {} type exception", exception.getType());

    switch (exception.getType()) {
      case FIELD_EXCEEDED, INVALID -> {
        this.synonymRegistry.invalidate(
            this.synonymMappingDefinition.name(), exception.getMessage());
        return;
      }
      case FAILED -> {
        this.synonymRegistry.fail(this.synonymMappingDefinition.name(), exception.getMessage());
        return;
      }
      case DROPPED -> {
        this.synonymRegistry.clear(this.synonymMappingDefinition);
        return;
      }
      // Don't fail/clear the mapping in case of shutdowns. The replication subsystem may be
      // restarting, and synonyms need to be queryable when replication is down. Mappings get
      // cleared by the garbage collector when the index gets dropped (or when mongot entirely
      // shuts down).
      case SHUTDOWN, TRANSIENT -> {
        // Don't drop or update the synonym registry in this case; the error was transient, and
        // doesn't indicate an invalid document or other synonym-mapping-invalidating circumstances.
        return;
      }
    }
  }
}
