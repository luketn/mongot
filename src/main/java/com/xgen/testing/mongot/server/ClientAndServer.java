package com.xgen.testing.mongot.server;

import org.bson.BsonDocument;

public interface ClientAndServer {
  void start(Mocks mocks);

  BsonDocument runCommand(BsonDocument command);

  void shutdownClient();

  void shutdownServer();
}
