package com.xgen.mongot.util.mongodb;

import com.google.common.annotations.VisibleForTesting;
import com.mongodb.client.MongoClient;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

public class MongoDbFsync {
  private static final String ADMIN_DATABASE_NAME = "admin";
  private static final String FSYNC_COMMAND_NAME = "fsync";

  @VisibleForTesting
  public static BsonDocument fsync(MongoClient mongoClient) {
    return mongoClient
        .getDatabase(ADMIN_DATABASE_NAME)
        .runCommand(new BsonDocument(FSYNC_COMMAND_NAME, new BsonInt32(1)), BsonDocument.class);
  }
}
