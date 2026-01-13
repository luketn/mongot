package com.xgen.mongot.server.grpc;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.ParsedCommand;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import io.grpc.stub.StreamObserver;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.RawBsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.RawBsonDocumentCodec;

/**
 * A handler class to process {@link RawBsonDocument} for the gRPC bidirectional streaming service.
 */
public class BsonMessageCallHandler extends ServerCallHandler<RawBsonDocument> {

  BsonMessageCallHandler(
      CommandRegistry commandRegistry,
      BulkheadCommandExecutor commandExecutor,
      MongotCursorManager cursorManager,
      SearchEnvoyMetadata searchEnvoyMetadata,
      StreamObserver<RawBsonDocument> responseObserver) {
    super(commandRegistry, commandExecutor, cursorManager, searchEnvoyMetadata, responseObserver);
  }

  /**
   * Relies on the command name to appear as the first key. We don't call BsonDocument::contains for
   * performance reasons
   */
  @Override
  ParsedCommand parseCommand(RawBsonDocument message) {
    return new ParsedCommand(message.getFirstKey(), message);
  }

  @Override
  RawBsonDocument serializeResponse(RawBsonDocument request, BsonDocument response) {
    return new RawBsonDocumentCodec()
        .decode(new BsonDocumentReader(response), DecoderContext.builder().build());
  }

  @Override
  RawBsonDocument serializeError(RawBsonDocument requestMessage, BsonDocument error) {
    return new RawBsonDocumentCodec()
        .decode(new BsonDocumentReader(error), DecoderContext.builder().build());
  }
}
