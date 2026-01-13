package com.xgen.mongot.util.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.util.mongodb.serialization.MongoDbInvalidReplStatusFormatException;
import com.xgen.mongot.util.mongodb.serialization.MongoDbReplSetGetStatusInfoProxy;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;

public class MongoDbReplSetStatus {

  private static final String ADMIN_DATABASE_NAME = "admin";
  private static final String REPL_SET_GET_STATUS_COMMAND_NAME = "replSetGetStatus";

  public static BsonTimestamp getLastCommittedOptime(MongoClient mongoClient)
      throws MongoDbInvalidReplStatusFormatException {
    return MongoDbReplSetStatus.getReplSetStatusInfo(mongoClient)
        .getOptimes()
        .getLastCommittedOpTime()
        .getTimestamp();
  }

  public static BsonTimestamp getReadConcernMajorityOpTime(MongoClient mongoClient)
      throws MongoDbInvalidReplStatusFormatException {
    return MongoDbReplSetStatus.getReplSetStatusInfo(mongoClient)
        .getOptimes()
        .getReadConcernMajorityOpTime()
        .getTimestamp();
  }

  /**
   * Returns the MongoDbReplSetGetStatusInfoProxy from calling replSetGetStatus, or empty if an
   * issue prevented it from being resolved.
   */
  private static MongoDbReplSetGetStatusInfoProxy getReplSetStatusInfo(MongoClient mongoClient)
      throws MongoDbInvalidReplStatusFormatException {
    return MongoDbReplSetGetStatusInfoProxy.fromBsonDocument(getReplSetStatus(mongoClient));
  }

  @VisibleForTesting
  public static BsonDocument getReplSetStatus(MongoClient mongoClient) {
    return mongoClient
        .getDatabase(ADMIN_DATABASE_NAME)
        .runCommand(
            new BsonDocument(REPL_SET_GET_STATUS_COMMAND_NAME, new BsonInt32(1)),
            BsonDocument.class);
  }
}
