package com.xgen.mongot.replication.mongodb.autoembedding;

import com.xgen.mongot.replication.ReplicationManager;
import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.util.Optional;

@FunctionalInterface
public interface AutoEmbeddingMaterializedViewManagerFactory {
  // TODO(CLOUDP-360913): Implement customized disk monitor for mat view.
  /**
   * Creates AutoEmbeddingMaterializedViewManager based on whether this node is leader or follower
   * role, and materialized view auto-embedding is enabled.
   */
  Optional<ReplicationManager> create(Optional<SyncSourceConfig> syncSourceConfig);
}
