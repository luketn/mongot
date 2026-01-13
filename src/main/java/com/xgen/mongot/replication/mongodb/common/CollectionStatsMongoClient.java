package com.xgen.mongot.replication.mongodb.common;

import com.mongodb.MongoNamespace;
import com.mongodb.client.MongoClient;
import com.xgen.mongot.util.mongodb.CollectionStatsCommand;
import com.xgen.mongot.util.mongodb.CollectionStatsResponse;
import com.xgen.mongot.util.mongodb.serialization.CodecRegistry;
import com.xgen.mongot.util.mongodb.serialization.CollectionStatsResponseProxy;

public class CollectionStatsMongoClient {

  private final MongoClient mongoClient;

  public CollectionStatsMongoClient(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  /** Returns collection statistics for the provided namespace. */
  public CollectionStatsResponse getStats(MongoNamespace namespace) {
    var command = new CollectionStatsCommand(namespace.getCollectionName());
    var proxy =
        this.mongoClient
            .getDatabase(namespace.getDatabaseName())
            .withCodecRegistry(CodecRegistry.PACKAGE_CODEC_REGISTRY)
            .runCommand(command.toProxy(), CollectionStatsResponseProxy.class);
    return CollectionStatsResponse.fromProxy(proxy);
  }
}
