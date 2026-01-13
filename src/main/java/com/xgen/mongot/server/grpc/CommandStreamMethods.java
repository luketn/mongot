package com.xgen.mongot.server.grpc;

import com.xgen.mongot.server.command.search.definition.request.SearchCommandDefinition;
import com.xgen.mongot.server.command.search.definition.request.VectorSearchCommandDefinition;
import com.xgen.mongot.server.message.MessageMessage;
import io.grpc.MethodDescriptor;
import org.bson.RawBsonDocument;

/**
 * This class defines the gRPC methods of the gRPC query server. Both client and server will be
 * implemented according to this definition.
 *
 * <p>All methods are bidirectional streaming.
 */
public class CommandStreamMethods {
  public static final String MONGODB_WIRE_SERVICE_NAME = "mongodb.CommandService";
  public static final String MONGODB_BSON_SERVICE_NAME = "mongodb.BsonCommandService";

  // This method will be triggered when mongod/mongos is speaking gRPC directly without auth.
  public static final MethodDescriptor<MessageMessage, MessageMessage>
      unauthenticatedCommandStream =
          createMethodDescriptor(
              MONGODB_WIRE_SERVICE_NAME, "UnauthenticatedCommandStream", new MessageMarshaller());

  // The current implementation of grpc server doesn't handle auth. However, the gRPC transcoding
  // plugin always sends an auth method: http://tinyurl.com/pwhna896
  public static final MethodDescriptor<MessageMessage, MessageMessage> authenticatedCommandStream =
      createMethodDescriptor(
          MONGODB_WIRE_SERVICE_NAME, "AuthenticatedCommandStream", new MessageMarshaller());

  public static final MethodDescriptor<RawBsonDocument, RawBsonDocument> searchCommandStream =
      createMethodDescriptor(
          MONGODB_BSON_SERVICE_NAME, SearchCommandDefinition.NAME, new RawBsonMarshaller());

  public static final MethodDescriptor<RawBsonDocument, RawBsonDocument> vectorSearchCommandStream =
      createMethodDescriptor(
          MONGODB_BSON_SERVICE_NAME, VectorSearchCommandDefinition.NAME, new RawBsonMarshaller());

  private static <T> MethodDescriptor<T, T> createMethodDescriptor(
      String serviceName, String methodName, MethodDescriptor.Marshaller<T> messageMarshaller) {
    return MethodDescriptor.<T, T>newBuilder()
        .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
        .setFullMethodName(serviceName + "/" + methodName)
        .setRequestMarshaller(messageMarshaller)
        .setResponseMarshaller(messageMarshaller)
        .build();
  }
}
