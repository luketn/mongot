package com.xgen.mongot.replication;

import com.xgen.mongot.util.mongodb.SyncSourceConfig;
import java.util.Optional;

@FunctionalInterface
public interface ReplicationManagerFactory {
  ReplicationManager create(Optional<SyncSourceConfig> syncSourceConfig);
}
