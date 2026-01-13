package com.xgen.mongot.server.command;

import org.bson.BsonDocument;

@FunctionalInterface
public interface CommandFactory extends CommandFactoryMarker {
  @Override
  default Type getType() {
    return Type.COMMAND_FACTORY;
  }

  Command create(BsonDocument args);
}
