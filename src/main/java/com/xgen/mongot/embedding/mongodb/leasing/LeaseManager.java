package com.xgen.mongot.embedding.mongodb.leasing;

import com.xgen.mongot.embedding.exceptions.MaterializedViewNonTransientException;
import com.xgen.mongot.embedding.exceptions.MaterializedViewTransientException;
import com.xgen.mongot.index.EncodedUserData;
import com.xgen.mongot.index.IndexGeneration;
import com.xgen.mongot.index.status.IndexStatus;
import com.xgen.mongot.index.version.GenerationId;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

// TODO(CLOUDP-371278): Refactor interface to have stricter type that only allows MatView related
// IndexGeneration and GenerationId
/** Interface for managing leases for the materialized view leader. */
public interface LeaseManager {

  /**
   * Adds a lease for the given index generation.
   *
   * @param indexGeneration the index generation to add the lease for.
   */
  void add(IndexGeneration indexGeneration);

  /**
   * Drops the given index generation from the lease. Deletes the lease entirely if this is the last
   * index generation in the lease.
   *
   * @param generationId the generation id to remove the lease for
   * @return a future that completes when the lease has been dropped
   */
  CompletableFuture<Void> drop(GenerationId generationId);

  /**
   * Returns true if the current mongot is the leader for the given generation id.
   *
   * @param generationId the generation id to check
   * @return true if the caller is the leader
   */
  boolean isLeader(GenerationId generationId);

  /**
   * Updates the commit info (replication checkpoint state) for the given generation id.
   *
   * @param generationId the generation id to update
   * @param encodedUserData the commit info to update
   */
  void updateCommitInfo(GenerationId generationId, EncodedUserData encodedUserData)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException;

  /**
   * Returns the commit info (replication checkpoint state) for the given generation id.
   *
   * @param generationId the generation id to get the commit info for
   * @return the commit info
   * @throws IOException if there are any issues talking to the lease store
   */
  EncodedUserData getCommitInfo(GenerationId generationId) throws IOException;

  /**
   * Updates the status of the materialized view replication.
   *
   * @param generationId the generation id of the index generation
   * @param indexDefinitionVersion the index definition version to update
   * @param indexStatus the status to update to
   */
  void updateReplicationStatus(
      GenerationId generationId, long indexDefinitionVersion, IndexStatus indexStatus)
      throws MaterializedViewTransientException, MaterializedViewNonTransientException;

  /**
   * Returns the status of the materialized view replication.
   *
   * @param indexGeneration the index generation to get the status for
   * @return the status of the leader. defaults to UNKNOWN if we're unable to determine the status.
   */
  IndexStatus getMaterializedViewReplicationStatus(IndexGeneration indexGeneration);
}
