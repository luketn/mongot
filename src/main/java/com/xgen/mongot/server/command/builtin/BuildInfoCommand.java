package com.xgen.mongot.server.command.builtin;

import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import java.util.Arrays;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

public class BuildInfoCommand implements Command {

  public static final String NAME = "buildInfo";
  public static final String ALT_NAME = "buildinfo";
  public static final CommandFactory FACTORY = (ignored) -> new BuildInfoCommand();

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public BsonDocument run() {
    return new BsonDocument()
        .append("version", new BsonString("4.2.0"))
        .append(
            "versionArray",
            new BsonArray(
                Arrays.asList(
                    new BsonInt32(4), new BsonInt32(2), new BsonInt32(0), new BsonInt32(0))))
        .append("ok", new BsonInt32(1));
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.SYNC;
  }
}
