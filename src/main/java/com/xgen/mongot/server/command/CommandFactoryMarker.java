package com.xgen.mongot.server.command;

public interface CommandFactoryMarker {
  enum Type {
    SESSION_COMMAND_FACTORY,
    COMMAND_FACTORY
  }

  Type getType();
}
