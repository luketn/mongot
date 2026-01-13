package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import org.bson.BsonTimestamp;

public interface ServerClusterTimeProvider {
  BsonTimestamp getCurrentClusterTime() throws InitialSyncException;
}
