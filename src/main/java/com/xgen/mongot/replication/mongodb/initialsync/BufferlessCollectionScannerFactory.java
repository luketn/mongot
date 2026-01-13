package com.xgen.mongot.replication.mongodb.initialsync;

import org.bson.BsonValue;

public interface BufferlessCollectionScannerFactory {

  BufferlessCollectionScanner create(InitialSyncContext context, BsonValue lastScannedToken);
}
