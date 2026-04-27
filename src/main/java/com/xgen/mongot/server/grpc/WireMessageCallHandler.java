package com.xgen.mongot.server.grpc;

import com.xgen.mongot.cursor.MongotCursorManager;
import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.server.command.ParsedCommand;
import com.xgen.mongot.server.command.WireCommandParser;
import com.xgen.mongot.server.command.registry.CommandRegistry;
import com.xgen.mongot.server.executors.BulkheadCommandExecutor;
import com.xgen.mongot.server.message.MessageHeader;
import com.xgen.mongot.server.message.MessageMessage;
import com.xgen.mongot.server.message.MessageSection;
import com.xgen.mongot.server.message.MessageSectionBody;
import com.xgen.mongot.server.message.MessageSectionDocumentSequence;
import io.grpc.stub.StreamObserver;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.RawBsonDocument;

/**
 * A handler class to process {@link MessageMessage} for the gRPC bidirectional streaming service.
 */
public class WireMessageCallHandler extends ServerCallHandler<MessageMessage> {

  WireMessageCallHandler(
      CommandRegistry commandRegistry,
      BulkheadCommandExecutor commandExecutor,
      MongotCursorManager cursorManager,
      SearchEnvoyMetadata searchEnvoyMetadata,
      StreamObserver<MessageMessage> responseObserver) {
    super(commandRegistry, commandExecutor, cursorManager, searchEnvoyMetadata, responseObserver);
  }

  @Override
  ParsedCommand parseCommand(MessageMessage message) {
    return WireCommandParser.parse(message);
  }

  @Override
  MessageMessage serializeResponse(MessageMessage request, BsonDocument response) {
    return (MessageMessage) request.getOutboundMessage(response);
  }

  @Override
  MessageMessage serializeError(MessageMessage request, BsonDocument error) {
    return (MessageMessage) request.getOutboundMessage(error);
  }

  @Override
  BsonDocument diagnosticMessage(MessageMessage message) {
    BsonArray sections = new BsonArray();
    for (MessageSection section : message.sections()) {
      sections.add(diagnosticSection(section));
    }

    return new BsonDocument()
        .append("header", diagnosticHeader(message.messageHeader()))
        .append("serializedSizeBytes", new BsonInt32(serializedSizeBytes(message)))
        .append("flagBits", new BsonInt32(message.flagBits()))
        .append("sections", sections);
  }

  @Override
  String diagnosticProtocol() {
    return "wire";
  }

  @Override
  String traceServiceName() {
    return CommandStreamMethods.MONGODB_WIRE_SERVICE_NAME;
  }

  private static BsonDocument diagnosticHeader(MessageHeader header) {
    return new BsonDocument()
        .append("messageLength", new BsonInt32(header.messageLength()))
        .append("requestId", new BsonInt32(header.requestId()))
        .append("responseTo", new BsonInt32(header.responseTo()))
        .append("opCode", new BsonString(header.opCode().name()))
        .append("opCodeNumber", new BsonInt32(header.opCode().code));
  }

  private static BsonDocument diagnosticSection(MessageSection section) {
    if (section instanceof MessageSectionBody body) {
      return new BsonDocument()
          .append("kind", new BsonString("body"))
          .append("body", body.body);
    }
    if (section instanceof MessageSectionDocumentSequence sequence) {
      BsonArray objects = new BsonArray();
      for (RawBsonDocument object : sequence.objects()) {
        objects.add(object);
      }
      return new BsonDocument()
          .append("kind", new BsonString("documentSequence"))
          .append("id", new BsonString(sequence.id()))
          .append("objects", objects);
    }
    throw new IllegalArgumentException("unknown message section " + section.getClass().getName());
  }

  private static int serializedSizeBytes(MessageMessage message) {
    return MessageHeader.SIZE_IN_BYTES
        + Integer.BYTES
        + message.sections().stream().mapToInt(MessageSection::size).sum();
  }
}
