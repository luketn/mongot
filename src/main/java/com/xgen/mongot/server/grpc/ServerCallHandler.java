package com.xgen.mongot.server.grpc;

import static com.xgen.mongot.server.command.CommandFactoryMarker.Type.COMMAND_FACTORY;
import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.base.Stopwatch;
import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.index.query.InvalidQueryException;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.Command;
import com.xgen.mongot.server.command.CommandFactory;
import com.xgen.mongot.server.command.ParsedCommand;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.server.executors.CancelledStreamSkipException;
import com.xgen.mongot.server.executors.LoadSheddingRejectedException;
import com.xgen.mongot.server.message.MessageUtils;
import com.xgen.mongot.trace.Tracing;
import com.xgen.mongot.util.FutureUtils;
import com.xgen.mongot.util.mongodb.Errors;
import io.grpc.stub.StreamObserver;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ServerCallHandler<T> implements StreamObserver<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ServerCallHandler.class);

  /**
   * Error labels for load shedding rejection responses. These labels follow the MongoDB wire
   * protocol convention defined in error_labels.h, allowing clients to identify transient overload
   * conditions and retry appropriately.
   */
  private static final List<String> LOAD_SHEDDING_ERROR_LABELS =
      List.of("SystemOverloadedError", "RetryableError");

  private final CommandRegistry commandRegistry;
  private final BulkheadCommandExecutor commandExecutor;
  private final MongotCursorManager cursorManager;
  private final CommandManager<T> commandManager;
  private final SearchEnvoyMetadata searchEnvoyMetadata;
  private final Stopwatch streamTime = Stopwatch.createStarted();
  private final AtomicBoolean streamTimerRecorded = new AtomicBoolean(false);
  private final Object waitSpanLock = new Object();
  private final AtomicInteger requestOrdinal = new AtomicInteger(0);
  private volatile Optional<Span> waitForClientMessageSpan = Optional.empty();
  private volatile Optional<CommandRegistry.CommandRegistration> initialCommandRegistration =
      Optional.empty();

  // Newly created cursors in this gRPC stream.
  // 1. This variable is at most written once. Each stream will have at most one $search command.
  // 2. This variable is at most read once during the cleanup callback in `onError`. When the
  //    cleanup callback is triggered, all commands in current gRPC stream has finished.
  volatile List<Long> createdCursorIds;

  ServerCallHandler(
      CommandRegistry commandRegistry,
      BulkheadCommandExecutor commandExecutor,
      MongotCursorManager cursorManager,
      SearchEnvoyMetadata searchEnvoyMetadata,
      StreamObserver<T> responseObserver) {
    this.commandRegistry = commandRegistry;
    this.commandExecutor = commandExecutor;
    this.cursorManager = cursorManager;
    this.searchEnvoyMetadata = searchEnvoyMetadata;
    this.commandManager = new CommandManager<T>(responseObserver, this::recordStreamTimer);
    this.createdCursorIds = Collections.emptyList();
    startWaitForClientMessageSpan("stream_open");
  }

  @Override
  public void onNext(T requestMsg) {
    int currentRequestOrdinal = this.requestOrdinal.incrementAndGet();
    endWaitForClientMessageSpan("command_received");
    try (var receiveSpan = Tracing.detailedSpanGuard("mongot.grpc.receive_message")) {
      receiveSpan.getSpan().setAttribute("mongot.grpc.request.ordinal", currentRequestOrdinal);
      this.commandManager.onCommandStart();
      Stopwatch totalTime = Stopwatch.createStarted();
      HandlingContext handlingContext = new HandlingContext();
      handleMessage(handlingContext, requestMsg)
          .whenComplete(
              (replyMsg, cause) -> {
                if (cause != null) {
                  Throwable unwrapped = FutureUtils.unwrapCause(cause);
                  this.commandManager.onCommandComplete(
                      getErrorMessage(requestMsg, cause),
                      () -> {
                        if (!(unwrapped instanceof InterruptedException)
                            && !(unwrapped instanceof CancelledStreamSkipException)) {
                          handlingContext.commandRegistration.ifPresent(
                              registration -> registration.failureCounter.increment());
                        }
                      });
                } else {
                  Stopwatch serializationTime = Stopwatch.createStarted();
                  this.commandManager.onCommandComplete(
                      replyMsg,
                      () -> {
                        // Update metrics after the message is sent.
                        handlingContext.commandRegistration.ifPresent(
                            commandRegistration -> {
                              commandRegistration.serializationTimer.ifPresent(
                                  t -> t.record(serializationTime.elapsed()));
                              commandRegistration.totalTimer.record(totalTime.elapsed());
                            });
                      });
                }
                if (this.requestOrdinal.get() == currentRequestOrdinal
                    && this.commandManager.isWaitingForClientMessages()) {
                  startWaitForClientMessageSpan("response_sent");
                }
              });
    }
  }

  CompletableFuture<T> handleMessage(HandlingContext handlingContext, T request) {
    try {
      ParsedCommand parsedCommand;
      try (var parseSpan = Tracing.detailedSpanGuard("mongot.grpc.parse_command")) {
        parsedCommand = parseCommand(request);
        parseSpan.getSpan().setAttribute("mongot.command.name", parsedCommand.name());
      }
      CommandRegistry.CommandRegistration registration;
      try (var lookupSpan =
          Tracing.detailedSpanGuard("mongot.grpc.lookup_command_registration")) {
        registration = this.commandRegistry.getCommandRegistration(parsedCommand.name());
        lookupSpan.getSpan().setAttribute("mongot.command.name", parsedCommand.name());
      }
      handlingContext.commandRegistration = Optional.of(registration);
      updateInitialServerSpan(parsedCommand);
      captureInitialCommandRegistration(registration);

      // Session commands are supposed to be handled by the Envoy proxy instead of the gRPC
      // server.
      checkArg(
          registration.factory.getType() == COMMAND_FACTORY,
          "do not know how to work with the command factory of %s",
          parsedCommand.name());

      // We don't check registration.isSecure here because the gRPC server will leverage mTLS
      // instead.
      Command command;
      try (var createSpan = Tracing.detailedSpanGuard("mongot.grpc.create_command")) {
        command = ((CommandFactory) registration.factory).create(parsedCommand.body());
        createSpan.getSpan().setAttribute("mongot.command.name", command.name());
        createSpan
            .getSpan()
            .setAttribute("mongot.command.depends_on_cursors", command.dependOnCursors());
      }

      // If this command depends on cursors but no cursors are created, throws an error.
      if (command.dependOnCursors() && this.createdCursorIds.isEmpty()) {
        throw new IllegalStateException("gRPC stream is broken");
      }
      command.handleSearchEnvoyMetadata(this.searchEnvoyMetadata);

      CompletableFuture<BsonDocument> commandResponse;
      try (var dispatchSpan = Tracing.detailedSpanGuard("mongot.grpc.dispatch_command")) {
        dispatchSpan.getSpan().setAttribute("mongot.command.name", command.name());
        dispatchSpan
            .getSpan()
            .setAttribute("mongot.command.execution_policy", command.getExecutionPolicy().name());
        commandResponse =
            this.commandExecutor.execute(command, this.commandManager::isStreamCancelled);
      }

      return commandResponse.thenApply(
          response -> {
            // If new cursors are created during command execution, track them.
            var createdCursorIds = command.getCreatedCursorIds();
            if (!createdCursorIds.isEmpty()) {
              this.createdCursorIds = createdCursorIds;
            }
            try (var serializeSpan = Tracing.detailedSpanGuard("mongot.grpc.serialize_response")) {
              serializeSpan.getSpan().setAttribute("mongot.command.name", command.name());
              serializeSpan.getSpan().setAttribute("mongot.response.field_count", response.size());
              return serializeResponse(request, response);
            }
          });
    } catch (Throwable t) {
      return CompletableFuture.failedFuture(t);
    }
  }

  @Override
  public void onError(Throwable t) {
    endWaitForClientMessageSpan("stream_error");
    try (var errorSpan = Tracing.detailedSpanGuard("mongot.grpc.stream_cancelled")) {
      errorSpan.getSpan().setAttribute("mongot.grpc.error.type", t.getClass().getName());
      this.commandManager.onStreamCancellation(
          () -> {
            // After sending half-close to the client, we will try to kill all the cursors that are
            // created in the gRPC stream.
            // Cursors may already be killed/exhausted. If a cursor is killed/exhausted,
            // `MongotCursorManager::killCursor` will be a no-op.
            this.createdCursorIds.forEach(
                cursorId -> {
                  this.cursorManager.killCursor(cursorId);
                });
          });
    }
  }

  @Override
  public void onCompleted() {
    endWaitForClientMessageSpan("client_half_closed");
    try (var ignored = Tracing.detailedSpanGuard("mongot.grpc.client_half_close")) {
      this.commandManager.onHalfClosedByClient();
    }
  }

  abstract ParsedCommand parseCommand(T message);

  abstract T serializeResponse(T request, BsonDocument response);

  abstract String traceServiceName();

  private synchronized void captureInitialCommandRegistration(
      CommandRegistry.CommandRegistration registration) {
    if (this.initialCommandRegistration.isEmpty()) {
      this.initialCommandRegistration = Optional.of(registration);
    }
  }

  private synchronized void updateInitialServerSpan(ParsedCommand parsedCommand) {
    if (this.initialCommandRegistration.isPresent()) {
      return;
    }

    Span span = Span.current();
    if (!span.getSpanContext().isValid()) {
      return;
    }

    Optional<String> namespace = traceNamespace(parsedCommand);
    if (namespace.isEmpty()) {
      return;
    }

    String operationName = parsedCommand.name();
    span.updateName("%s/%s/%s".formatted(traceServiceName(), namespace.get(), operationName));
    span.setAttribute("db.system", "mongodb");
    span.setAttribute("db.operation.name", operationName);
    span.setAttribute("db.namespace", namespace.get());
    span.setAttribute("mongot.command.name", operationName);
  }

  private static Optional<String> traceNamespace(ParsedCommand parsedCommand) {
    Optional<String> db = stringField(parsedCommand.body(), "$db");
    Optional<String> collection =
        stringField(parsedCommand.body(), parsedCommand.name())
            .or(() -> stringField(parsedCommand.body(), "search"))
            .or(() -> stringField(parsedCommand.body(), "searchBeta"))
            .or(() -> stringField(parsedCommand.body(), "vectorSearch"));
    if (db.isEmpty() || collection.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(db.get() + "." + collection.get());
  }

  private static Optional<String> stringField(BsonDocument document, String fieldName) {
    BsonValue value = document.get(fieldName);
    if (value == null || !value.isString()) {
      return Optional.empty();
    }
    return Optional.of(value.asString().getValue());
  }

  private void recordStreamTimer() {
    if (!this.streamTimerRecorded.compareAndSet(false, true)) {
      return;
    }
    this.initialCommandRegistration.ifPresent(
        registration ->
            registration.streamTimer.ifPresent(timer -> timer.record(this.streamTime.elapsed())));
  }

  private void startWaitForClientMessageSpan(String startReason) {
    if (!Tracing.isDetailedTraceSpansEnabled()) {
      return;
    }
    synchronized (this.waitSpanLock) {
      if (this.waitForClientMessageSpan.isPresent()) {
        return;
      }
      this.waitForClientMessageSpan =
          Tracing.detailedUnguardedSpan(
              "mongot.grpc.wait_for_client_message",
              Attributes.builder()
                  .put("mongot.grpc.wait.start_reason", startReason)
                  .put("mongot.grpc.next_request.ordinal", this.requestOrdinal.get() + 1)
                  .build());
    }
  }

  private void endWaitForClientMessageSpan(String endReason) {
    Optional<Span> spanToEnd;
    synchronized (this.waitSpanLock) {
      spanToEnd = this.waitForClientMessageSpan;
      this.waitForClientMessageSpan = Optional.empty();
    }
    spanToEnd.ifPresent(
        span -> {
          span.setAttribute("mongot.grpc.wait.end_reason", endReason);
          span.end();
        });
  }

  private T getErrorMessage(T request, Throwable exception) {
    Throwable cause = FutureUtils.unwrapCause(exception);

    // Load shedding rejection should include error code and labels for client retry handling
    if (cause instanceof LoadSheddingRejectedException) {
      String message =
          cause.getMessage() == null ? "Server is at capacity" : cause.getMessage();
      BsonDocument error =
          MessageUtils.createErrorBodyWithLabels(
              message, LOAD_SHEDDING_ERROR_LABELS, Errors.INGRESS_REQUEST_RATE_LIMIT_EXCEEDED);
      return serializeError(request, error);
    }

    if (!(cause instanceof InvalidQueryException)
        && !(cause instanceof CancelledStreamSkipException)) {
      LOG.warn("unexpected exception", cause);
    }

    BsonDocument error =
        MessageUtils.createErrorBody(
            cause.getMessage() == null ? "unknown error" : cause.getMessage());

    return serializeError(request, error);
  }

  abstract T serializeError(T request, BsonDocument error);

  static class HandlingContext {
    // After command execution, corresponding metrics in the following registration will be updated.
    Optional<CommandRegistry.CommandRegistration> commandRegistration = Optional.empty();
  }
}
