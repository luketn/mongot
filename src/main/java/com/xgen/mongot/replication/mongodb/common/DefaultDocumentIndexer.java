package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.logging.DefaultKeyValueLogger;
import com.xgen.mongot.util.Crash;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class DefaultDocumentIndexer implements DocumentIndexer {

  private final DefaultKeyValueLogger logger;
  private final InitializedIndex index;
  private final IndexDefinition indexDefinition;
  private final AtomicReference<IndexCommitUserData> commitUserData;

  private DefaultDocumentIndexer(InitializedIndex index, IndexDefinition indexDefinition) {
    HashMap<String, Object> defaultKeyValues = new HashMap<>();
    defaultKeyValues.put("indexId", indexDefinition.getIndexId());
    this.logger = DefaultKeyValueLogger.getLogger(DefaultDocumentIndexer.class, defaultKeyValues);
    this.index = index;
    this.indexDefinition = indexDefinition;
    this.commitUserData =
        new AtomicReference<>(
            IndexCommitUserData.fromEncodedData(
                index.getWriter().getCommitUserData(), Optional.of(index.getGenerationId())));
  }

  public static DefaultDocumentIndexer create(InitializedIndex index) {
    return new DefaultDocumentIndexer(index, index.getDefinition());
  }

  @Override
  public void indexDocumentEvent(DocumentEvent event) throws FieldExceededLimitsException {

    try {
      this.index.getWriter().updateIndex(event);

    } catch (IOException e) {
      Crash.because(
              String.format(
                  "index %s failed to update index",
                  this.indexDefinition.getIndexId().toHexString()))
          .withThrowable(e)
          .now();
    }
  }

  @Override
  public void updateCommitUserData(IndexCommitUserData commitUserData) {
    this.commitUserData.set(commitUserData);
  }

  @Override
  public synchronized void commit() throws IOException {
    IndexCommitUserData userData = this.commitUserData.get();
    this.index.getWriter().commit(userData.toEncodedData());
  }

  @Override
  public synchronized void clearIndex(IndexCommitUserData commitUserData) {
    this.index.clear(commitUserData.toEncodedData());
    updateCommitUserData(commitUserData);
  }

  @Override
  public Optional<ExceededLimitsException> exceededLimits() {
    Optional<ExceededLimitsException> maybeExceeded = this.index.getWriter().exceededLimits();
    maybeExceeded.ifPresent(
        exceeded -> this.logger.info("Index exceeded limits: {}", exceeded.getMessage()));
    return maybeExceeded;
  }

  @Override
  public IndexDefinition getIndexDefinition() {
    return this.indexDefinition;
  }
}
