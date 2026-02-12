package com.xgen.mongot.catalogservice;

import java.util.List;
import java.util.Set;
import org.bson.BsonDocument;

public interface IndexStats {

  /** Stores an index stats entry in the metadata collection */
  void insert(IndexStatsEntry indexStats) throws MetadataServiceException;

  /** Upserts all the index stats entries in the provided list into the metadata collection */
  void upsertAll(Set<IndexStatsEntry> indexStats) throws MetadataServiceException;

  /** Removes an index stats entry from the metadata collection */
  void delete(IndexStatsEntry.IndexStatsKey key) throws MetadataServiceException;

  /**
   * Removes the index stats entries from the metadata collection for each indexStatsKey in the
   * provided list.
   */
  void deleteAll(Set<IndexStatsEntry.IndexStatsKey> indexStatsKeys) throws MetadataServiceException;

  /** Lists the index stats entries from the metadata collection based on a filter */
  List<IndexStatsEntry> list(BsonDocument filter) throws MetadataServiceException;

  /** Lists all index stats entries from the metadata collection */
  List<IndexStatsEntry> list() throws MetadataServiceException;

  /**
   * Creates the default indexes for the index stats mongod metadata collection. This operation is
   * idempotent and can be called multiple times.
   *
   * <p>Note that this should not be called during server bootstrap as it requires the mongod
   * instance to be available before the mongot process which we can't guarantee.
   */
  void createCollectionIndexes() throws MetadataServiceException;
}
