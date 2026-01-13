package com.xgen.mongot.catalog;

import com.xgen.mongot.index.IndexGeneration;
import java.util.Collection;
import java.util.Optional;
import java.util.UUID;
import org.bson.types.ObjectId;

/**
 * IndexCatalog provides a thread safe interface for adding, removing, and inspecting existing
 * Indexes.
 *
 * <p>Note that while adding and retrieving indexes is thread safe, there are no further guarantees
 * when it comes to the Indexes returned from the IndexCatalog.
 *
 * <p>For example, getIndexes() will return the Indexes in the IndexCatalog at the time of calling,
 * but that does not prevent another thread from removing one of the returned Indexes later while
 * you are processing the results of getIndexes().
 *
 * <p>If this behavior is required you must use higher level synchronization.
 */
public interface IndexCatalog {

  /** Adds the given IndexGeneration to the IndexCatalog. */
  void addIndex(IndexGeneration indexGeneration);

  /** Retrieves the search Index by the provided properties. */
  Optional<IndexGeneration> getIndex(
      String databaseName, UUID collectionUuid, Optional<String> viewName, String indexName);

  /** Retrieves the Index with the given id if one exists. */
  Optional<IndexGeneration> getIndexById(ObjectId indexId);

  /**
   * Gets a snapshot of all the Indexes currently in the Catalog. Catalog can be immediately changed
   * by other threads right after returning the result.
   */
  Collection<IndexGeneration> getIndexes();

  /**
   * Gets the total number of indexes in the Catalog. This method is thread-safe and can be used for
   * reporting or monitoring purposes.
   */
  int getSize();

  /**
   * Removes an Index with the given id from the IndexCatalog, returning the Index if it did exist.
   */
  Optional<IndexGeneration> removeIndex(ObjectId indexId);
}
