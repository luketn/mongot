package com.xgen.mongot.replication;

import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.InitializedIndex;
import com.xgen.mongot.index.version.GenerationId;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * ReplicationManager is in charge of the replication of Indexes - replicating them from their
 * source, and handling events like the source being dropped.
 *
 * <p>TODO(CLOUDP-231027): Separate this type from LifecycleManager once all config worfklows flow
 * through the LifecycleManager.
 */
public interface ReplicationManager {

  /**
   * Starts replication of the supplied Index.
   *
   * <p>TODO(CLOUDP-231027): Pass {@link InitializedIndex} as a parameter after separating from
   * LifecycleManager
   */
  void add(IndexGeneration indexGeneration);

  /**
   * Stops replication of the index with the supplied id, and drops the index.
   *
   * @param generationId the id of the index to drop
   * @return a future that completes when the index has been dropped. The future will only ever
   *     complete successfully.
   */
  CompletableFuture<Void> dropIndex(GenerationId generationId);

  /**
   * Returns the MongoDb and MongoS ConnectionStrings that this LifecycleManager is replicating
   * from.
   */
  Optional<SyncSourceConfig> getSyncSourceConfig();

  boolean isInitialized();

  /**
   * Return whether the replication is supported. No-op ReplicationManager will always return false
   * and normal ReplicationManager will always return true
   */
  boolean isReplicationSupported();

  /**
   * Stops the ReplicationManager, gracefully shutting down all indexes.
   *
   * @return a future that completes when the ReplicationManager has completed shutting down. The
   *     future will only ever complete successfully.
   */
  CompletableFuture<Void> shutdown();
}
