package com.xgen.mongot.server.command.builtin;

import com.mongodb.AuthenticationMechanism;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;

public class IsMasterCommand implements Command {

  public static final String NAME = "isMaster";
  public static final String ALT_NAME = "ismaster";
  public static final CommandFactory FACTORY = (ignored) -> new IsMasterCommand();

  // Mongo server uses wire version to determine if driver/mongod/mongos can interact,
  // It represents the message syntax and logical capabilities. See HELP-22883 for more information.
  // Setting the range to [0, max int) means that any client can interact with mongot. The changes
  // in the mongod-mongot protocol are not guarded by this protocol version. So this constraint
  // is mostly irrelevant for mongot.
  private static final int MIN_WIRE_VERSION = 0;
  private static final int MAX_WIRE_VERSION = Integer.MAX_VALUE - 1;

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public BsonDocument run() {
    // Note that we do not presently report helloOk: true as required by the spec
    // (https://tinyurl.com/2s4ysxfy) even though hello is supported in order to minimize changes in
    // behavior. The server may itself decide to use hello to initiate handshakes if it deems it
    // appropriate.
    return new BsonDocument()
        .append("ismaster", BsonBoolean.TRUE)
        .append("maxBsonObjectSize", new BsonInt32(16777216))
        .append("maxMessageSizeBytes", new BsonInt32(48000000))
        .append("maxWriteBatchSize", new BsonInt32(100000))
        .append("localTime", new BsonInt64(System.currentTimeMillis()))
        .append("minWireVersion", new BsonInt32(MIN_WIRE_VERSION))
        .append("maxWireVersion", new BsonInt32(MAX_WIRE_VERSION))
        .append("readOnly", BsonBoolean.FALSE)
        .append(
            "saslSupportedMechs",
            new BsonArray(
                Stream.of(
                        AuthenticationMechanism.SCRAM_SHA_1, AuthenticationMechanism.SCRAM_SHA_256)
                    .map(AuthenticationMechanism::getMechanismName)
                    .map(BsonString::new)
                    .collect(Collectors.toList())))
        .append("ok", new BsonInt32(1));
  }

  @Override
  public ExecutionPolicy getExecutionPolicy() {
    return ExecutionPolicy.SYNC;
  }
}
