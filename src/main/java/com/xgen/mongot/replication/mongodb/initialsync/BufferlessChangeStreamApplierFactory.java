package com.xgen.mongot.replication.mongodb.initialsync;

import com.xgen.mongot.replication.mongodb.common.InitialSyncException;
import org.bson.BsonTimestamp;

interface BufferlessChangeStreamApplierFactory {

  BufferlessChangeStreamApplier create(BsonTimestamp highWaterMark) throws InitialSyncException;
}
