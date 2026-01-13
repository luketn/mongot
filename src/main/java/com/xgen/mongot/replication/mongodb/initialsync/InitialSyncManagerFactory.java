package com.xgen.mongot.replication.mongodb.initialsync;

import com.mongodb.MongoNamespace;
import com.xgen.mongot.replication.mongodb.common.InitialSyncResumeInfo;
import java.util.Optional;

interface InitialSyncManagerFactory {

  InitialSyncManager create(
      InitialSyncContext context,
      InitialSyncMongoClient mongoClient,
      MongoNamespace namespace,
      Optional<InitialSyncResumeInfo> resumeInfo);
}
