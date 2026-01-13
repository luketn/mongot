package com.xgen.mongot.util.mongodb;

@FunctionalInterface
public interface MongoDbServerInfoProvider {
  MongoDbServerInfo getCachedMongoDbServerInfo();
}
