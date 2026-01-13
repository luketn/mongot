package com.xgen.mongot.replication.mongodb.common;

import com.xgen.mongot.index.DocumentEvent;
import com.xgen.mongot.index.ExceededLimitsException;
import com.xgen.mongot.index.FieldExceededLimitsException;
import com.xgen.mongot.index.definition.IndexDefinition;
import java.io.IOException;
import java.util.Optional;

/**
 * An interface for indexing and commit. Implementations should be thread-safe.
 *
 * <ul>
 *   Implementation should also make sure that the following methods guarantee a serialized
 *   execution when called concurrently:
 *   <li>{@link DocumentIndexer#commit}
 *   <li>{@link DocumentIndexer#clearIndex}
 * </ul>
 */
public interface DocumentIndexer {

  void indexDocumentEvent(DocumentEvent event) throws FieldExceededLimitsException;

  /**
   * Updates live commit user data, which will be persisted along with the indexed documents at
   * commit time. This method provides "last write wins" semantics, so clients should take into
   * account that concurrent calls might cause unexpected results.
   */
  void updateCommitUserData(IndexCommitUserData commitUserData);

  /** Commits all pending changes to the index. */
  void commit() throws IOException;

  /** Clear the index and update the commit user data atomically. */
  void clearIndex(IndexCommitUserData commitUserData) throws IOException;

  Optional<ExceededLimitsException> exceededLimits();

  IndexDefinition getIndexDefinition();
}
