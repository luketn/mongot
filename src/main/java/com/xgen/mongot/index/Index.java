package com.xgen.mongot.index;

import com.xgen.mongot.index.definition.IndexDefinition;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.util.Check;
import java.io.Closeable;
import java.io.IOException;

/**
 * Couples an IndexDefinition with an IndexReader and IndexWriter that can index and service queries
 * based on the definition.
 */
public interface Index extends Closeable {

  IndexDefinition getDefinition();

  void setStatus(IndexStatus status);

  /** The {@link IndexStatus} for the main search index of this {@link Index}. */
  IndexStatus getStatus();

  /** Returns true if the Index is compatible with the given type of {@link IndexDefinition}. */
  boolean isCompatibleWith(IndexDefinition indexDefinition);

  /**
   * Drops the index, completely deleting it from disk.
   *
   * <p>NOTE: the Index must be closed first.
   */
  void drop() throws IOException;

  boolean isClosed();

  void throwIfUnavailableForQuerying() throws IndexUnavailableException;

  default SearchIndex asSearchIndex() {
    return Check.instanceOf(this, SearchIndex.class);
  }

  default VectorIndex asVectorIndex() {
    return Check.instanceOf(this, VectorIndex.class);
  }
}
