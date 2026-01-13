package com.xgen.mongot.config.manager;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.collect.HashMultimap;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.version.GenerationId;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.bson.types.ObjectId;

/**
 * A collection of IndexGenerations with unique GenerationId (indexIds may not be unique), this
 * class is not thread safe.
 */
public class PhasingOutIndexes {
  private final ConcurrentHashMap<GenerationId, IndexGeneration> indexes =
      new ConcurrentHashMap<>();

  /** Multiple indexes may be present for one indexId. */
  private final HashMultimap<ObjectId, IndexGeneration> byIndexId = HashMultimap.create();

  /** Adds an index to this collection. */
  public void addIndex(IndexGeneration indexGeneration) {
    GenerationId id = indexGeneration.getGenerationId();
    checkState(!this.indexes.containsKey(id), "already contains index of id: %s", id);
    this.indexes.put(id, indexGeneration);
    this.byIndexId.put(id.indexId, indexGeneration);
  }

  public List<IndexGeneration> getIndexes() {
    return new ArrayList<>(this.indexes.values());
  }

  /**
   * Gets the total number of indexes. This method is thread-safe and can be used for reporting or
   * monitoring purposes.
   */
  int getSize() {
    return this.indexes.size();
  }

  public List<IndexGeneration> getIndexesById(ObjectId indexId) {
    // making a protective copy, MultiMap::get returns an update-able view.
    return new ArrayList<>(this.byIndexId.get(indexId));
  }

  /** returns true if the index was in the collection. */
  public boolean removeIndex(IndexGeneration indexGeneration) {
    GenerationId id = indexGeneration.getGenerationId();
    this.byIndexId.remove(id.indexId, indexGeneration);
    return Optional.ofNullable(this.indexes.remove(id)).isPresent();
  }
}
