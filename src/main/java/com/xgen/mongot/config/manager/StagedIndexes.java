package com.xgen.mongot.config.manager;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.index.IndexGeneration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.NotNull;

/**
 * A collection of IndexGenerations with unique ObjectId IndexIds. This class is not thread-safe.
 */
public class StagedIndexes {
  private final ConcurrentHashMap<ObjectId, IndexGeneration> indexes = new ConcurrentHashMap<>();

  /** Adds an index to this collection. */
  public void addIndex(IndexGeneration indexGeneration) {
    ObjectId id = indexGeneration.getDefinition().getIndexId();
    checkState(!this.indexes.containsKey(id), "already contains index of id: %s", id);
    this.indexes.put(id, indexGeneration);
  }

  public Collection<@NotNull IndexGeneration> getIndexes() {
    return new ArrayList<>(this.indexes.values());
  }

  /** returns true if the index was in the collection. */
  public boolean removeIndex(IndexGeneration index) {
    return Optional.ofNullable(this.indexes.remove(index.getDefinition().getIndexId())).isPresent();
  }

  public Optional<IndexGeneration> getIndex(ObjectId indexId) {
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
