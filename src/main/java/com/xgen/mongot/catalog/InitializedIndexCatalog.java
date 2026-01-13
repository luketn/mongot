package com.xgen.mongot.catalog;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.version.GenerationId;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A collection of IndexGenerations with unique GenerationId (indexIds may not be unique). This
 * class is not thread-safe.
 */
public class InitializedIndexCatalog {
  private final Map<GenerationId, InitializedIndex> indexes;

  public InitializedIndexCatalog() {
    this.indexes = new ConcurrentHashMap<>();
  }

  /** Adds an index to this collection. */
  public void addIndex(InitializedIndex initializedIndex) {
    GenerationId generationId = initializedIndex.getGenerationId();
    checkState(
        !this.indexes.containsKey(generationId), "already contains index of id: %s", generationId);
    this.indexes.put(generationId, initializedIndex);
  }

  /** Remove the index from the collection. */
  public Optional<InitializedIndex> removeIndex(GenerationId generationId) {
    return Optional.ofNullable(this.indexes.remove(generationId));
  }

  /** Return the initialized index if present. */
  public Optional<InitializedIndex> getIndex(GenerationId indexId) {
    return Optional.ofNullable(this.indexes.get(indexId));
  }

  /**
   * Gets the total number of indexes. This method is thread-safe and can be used for reporting or
   * monitoring purposes.
   */
  int getSize() {
    return this.indexes.size();
  }
}
