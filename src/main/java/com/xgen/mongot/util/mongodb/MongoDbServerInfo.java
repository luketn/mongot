package com.xgen.mongot.util.mongodb;

import java.util.Optional;

public record MongoDbServerInfo(Optional<MongoDbVersion> mongoDbVersion, Optional<String> rsId) {
  public static final MongoDbServerInfo EMPTY =
      new MongoDbServerInfo(Optional.empty(), Optional.empty());
}
