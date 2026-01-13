package com.xgen.mongot.server.command.builtin;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

public class PingCommand implements Command {

  public static final String NAME = "ping";
  public static final CommandFactory FACTORY = (ignored) -> new PingCommand();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public BsonDocument run() {
    return new BsonDocument().append("ok", new BsonInt32(1));
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.SYNC;
  }
}
