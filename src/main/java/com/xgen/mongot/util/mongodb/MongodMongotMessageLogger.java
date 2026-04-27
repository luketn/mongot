package com.xgen.mongot.util.mongodb;

import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Slow, opt-in diagnostic logger for actual MongoD/MongoT message payloads.
 *
 * <p>This is intentionally disabled by default. When enabled by {@link #ENV_VAR}, it writes one
 * JSON document per line and flushes every event so short-lived documentation captures do not lose
 * the tail of the exchange.
 */
public final class MongodMongotMessageLogger {
  public static final String ENV_VAR = "MONGOT_MONGOD_MESSAGE_LOG_JSONL";

  private static final Logger LOG = LoggerFactory.getLogger(MongodMongotMessageLogger.class);
  private static final String DEFAULT_PATH = "mongod-mongot-messages.jsonl";
  private static final JsonWriterSettings JSON_SETTINGS =
      JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build();
  private static final MongodMongotMessageLogger INSTANCE = create();

  private final Optional<BufferedWriter> writer;
  private final CommandListener commandListener;
  private volatile boolean active;

  private MongodMongotMessageLogger(Optional<BufferedWriter> writer) {
    this.writer = writer;
    this.commandListener = new DiagnosticCommandListener(this);
    this.active = writer.isPresent();
  }

  public static MongodMongotMessageLogger get() {
    return INSTANCE;
  }

  public boolean isActive() {
    return this.active;
  }

  public CommandListener commandListener() {
    return this.commandListener;
  }

  public void logGrpcMessage(
      int streamId,
      int requestOrdinal,
      String direction,
      String protocol,
      Optional<String> commandName,
      boolean error,
      BsonDocument message) {
    if (!this.active) {
      return;
    }

    BsonDocument event =
        new BsonDocument()
            .append("timestamp", new BsonString(Instant.now().toString()))
            .append("transport", new BsonString("mongod-mongot-grpc"))
            .append("direction", new BsonString(direction))
            .append("protocol", new BsonString(protocol))
            .append("streamId", new BsonInt32(streamId))
            .append("requestOrdinal", new BsonInt32(requestOrdinal))
            .append("error", BsonBoolean.valueOf(error))
            .append("message", message);
    commandName.ifPresent(name -> event.append("commandName", new BsonString(name)));
    write(event);
  }

  private void logCommandStarted(CommandStartedEvent event) {
    if (!this.active) {
      return;
    }

    write(
        driverEventBase(
                "mongot_to_mongod",
                event.getCommandName(),
                event.getRequestId(),
                event.getOperationId(),
                event.getConnectionDescription().toString())
            .append("databaseName", new BsonString(event.getDatabaseName()))
            .append("message", new BsonDocument("command", event.getCommand())));
  }

  private void logCommandSucceeded(CommandSucceededEvent event) {
    if (!this.active) {
      return;
    }

    write(
        driverEventBase(
                "mongod_to_mongot",
                event.getCommandName(),
                event.getRequestId(),
                event.getOperationId(),
                event.getConnectionDescription().toString())
            .append("durationNanos", new BsonInt64(event.getElapsedTime(TimeUnit.NANOSECONDS)))
            .append("message", new BsonDocument("reply", event.getResponse())));
  }

  private void logCommandFailed(CommandFailedEvent event) {
    if (!this.active) {
      return;
    }

    Throwable throwable = event.getThrowable();
    write(
        driverEventBase(
                "mongod_to_mongot",
                event.getCommandName(),
                event.getRequestId(),
                event.getOperationId(),
                event.getConnectionDescription().toString())
            .append("durationNanos", new BsonInt64(event.getElapsedTime(TimeUnit.NANOSECONDS)))
            .append("error", BsonBoolean.TRUE)
            .append(
                "message",
                new BsonDocument()
                    .append("failureClass", new BsonString(throwable.getClass().getName()))
                    .append(
                        "failureMessage",
                        new BsonString(
                            Optional.ofNullable(throwable.getMessage()).orElse("")))));
  }

  private static BsonDocument driverEventBase(
      String direction,
      String commandName,
      int requestId,
      long operationId,
      String connectionDescription) {
    return new BsonDocument()
        .append("timestamp", new BsonString(Instant.now().toString()))
        .append("transport", new BsonString("mongot-java-driver"))
        .append("direction", new BsonString(direction))
        .append("protocol", new BsonString("mongodb-wire"))
        .append("commandName", new BsonString(commandName))
        .append("requestId", new BsonInt32(requestId))
        .append("operationId", new BsonInt64(operationId))
        .append("connectionDescription", new BsonString(connectionDescription))
        .append("error", BsonBoolean.FALSE);
  }

  private synchronized void write(BsonDocument event) {
    if (!this.active || this.writer.isEmpty()) {
      return;
    }

    try {
      this.writer.get().write(event.toJson(JSON_SETTINGS));
      this.writer.get().newLine();
      this.writer.get().flush();
    } catch (Throwable t) {
      this.active = false;
      LOG.warn("Disabling MongoD/MongoT diagnostic message logging after write failure.", t);
    }
  }

  private static MongodMongotMessageLogger create() {
    String value = System.getenv(ENV_VAR);
    if (value == null || value.isBlank() || value.equalsIgnoreCase("false") || value.equals("0")) {
      return new MongodMongotMessageLogger(Optional.empty());
    }

    Path path = Path.of(value.equalsIgnoreCase("true") || value.equals("1") ? DEFAULT_PATH : value);
    try {
      Path parent = path.toAbsolutePath().getParent();
      if (parent != null) {
        Files.createDirectories(parent);
      }
      BufferedWriter writer =
          Files.newBufferedWriter(
              path,
              StandardCharsets.UTF_8,
              StandardOpenOption.CREATE,
              StandardOpenOption.APPEND);
      LOG.warn(
          "Enabled slow MongoD/MongoT diagnostic message logging. {}={}",
          ENV_VAR,
          path.toAbsolutePath());
      return new MongodMongotMessageLogger(Optional.of(writer));
    } catch (IOException e) {
      LOG.warn(
          "Failed to enable MongoD/MongoT diagnostic message logging. {}={}",
          ENV_VAR,
          path.toAbsolutePath(),
          e);
      return new MongodMongotMessageLogger(Optional.empty());
    }
  }

  private static final class DiagnosticCommandListener implements CommandListener {
    private final MongodMongotMessageLogger logger;

    private DiagnosticCommandListener(MongodMongotMessageLogger logger) {
      this.logger = logger;
    }

    @Override
    public void commandStarted(CommandStartedEvent event) {
      this.logger.logCommandStarted(event);
    }

    @Override
    public void commandSucceeded(CommandSucceededEvent event) {
      this.logger.logCommandSucceeded(event);
    }

    @Override
    public void commandFailed(CommandFailedEvent event) {
      this.logger.logCommandFailed(event);
    }
  }
}
