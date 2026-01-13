package com.xgen.mongot.server.grpc;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;
import java.util.Set;

/**
 * This interceptor implements the MongoDB gRPC protocol
 *
 * <p>MongoDB Server implementation can be found here:
 * https://github.com/mongodb/mongo/blob/master/src/mongo/transport/grpc/service.cpp
 */
public class MongoDbGrpcProtocolInterceptor implements ServerInterceptor {
  // To ensure no downtime during upgrade/downgrade, mongot needs to update
  // MONGODB_ACTUAL_MAX_WIRE_VERSION in one version and update MONGODB_REPORT_MAX_WIRE_VERSION is a
  // future version.
  public static final int MONGODB_ACTUAL_MAX_WIRE_VERSION = 26; // MongoDB 8.1
  public static final int MONGODB_REPORT_MAX_WIRE_VERSION = 26; // MongoDB 8.1

  private static final Set<String> allowedMongoDbRequestHeaders =
      Set.of(
          GrpcMetadata.MONGODB_WIRE_VERSION_METADATA_KEY.name(),
          GrpcMetadata.MONGODB_CLIENT_ID_METADATA_KEY.name(),
          GrpcMetadata.MONGODB_CLIENT_METADATA_KEY.name());

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata requestHeaders, ServerCallHandler<ReqT, RespT> next) {
    if (!call.getMethodDescriptor()
        .getServiceName()
        .equals(CommandStreamMethods.MONGODB_WIRE_SERVICE_NAME)) {
      // This interceptor won't do anything on non-mongodb service. (e.g. Health checks)
      return next.startCall(call, requestHeaders);
    }

    Status status = checkRequestHeaders(requestHeaders);
    if (!status.isOk()) {
      // We always need to provide the max wire version in the header.
      Metadata responseHeaders = new Metadata();
      populateResponseHeaders(responseHeaders);
      call.sendHeaders(responseHeaders);
      call.close(status, new Metadata());
      return new ServerCall.Listener<>() {};
    }

    return next.startCall(
        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
          @Override
          public void sendHeaders(Metadata responseHeaders) {
            populateResponseHeaders(responseHeaders);
            super.sendHeaders(responseHeaders);
          }
        },
        requestHeaders);
  }

  private static Status checkRequestHeaders(Metadata requestHeaders) {
    Optional<Integer> mongodbWireVersion;
    try {
      mongodbWireVersion =
          Optional.ofNullable(requestHeaders.get(GrpcMetadata.MONGODB_WIRE_VERSION_METADATA_KEY));
    } catch (NumberFormatException e) {
      var asciiKey =
          Metadata.Key.of(
              GrpcMetadata.MONGODB_WIRE_VERSION_METADATA_KEY.name(),
              Metadata.ASCII_STRING_MARSHALLER);
      return Status.INVALID_ARGUMENT.withDescription(
          String.format("Invalid wire version: \"%s\"", requestHeaders.get(asciiKey)));
    }
    if (mongodbWireVersion.isEmpty()
        || mongodbWireVersion.get() > MONGODB_ACTUAL_MAX_WIRE_VERSION) {
      // Returns FAILED_PRECONDITION error if the wire version is invalid.

      String errorMessage =
          mongodbWireVersion
              .map(
                  providedWireVersion ->
                      String.format(
                          "Provided wire version (%d) exceeds cluster's max wire version (%d)",
                          providedWireVersion, MONGODB_ACTUAL_MAX_WIRE_VERSION))
              .orElseGet(
                  () ->
                      String.format(
                          "Clients must specify the server wire version they are targeting in "
                              + "the \"%s\" metadata entry",
                          GrpcMetadata.MONGODB_WIRE_VERSION_METADATA_KEY.name()));
      return Status.FAILED_PRECONDITION.withDescription(errorMessage);
    }

    for (String key : requestHeaders.keys()) {
      if (key.startsWith("mongodb-") && !allowedMongoDbRequestHeaders.contains(key)) {
        return Status.INVALID_ARGUMENT.withDescription(
            String.format("Unrecognized reserved metadata key: \"%s\"", key));
      }
    }

    try {
      requestHeaders.get(GrpcMetadata.MONGODB_CLIENT_ID_METADATA_KEY);
    } catch (IllegalArgumentException e) {
      var asciiKey =
          Metadata.Key.of(
              GrpcMetadata.MONGODB_CLIENT_ID_METADATA_KEY.name(), Metadata.ASCII_STRING_MARSHALLER);
      return Status.INVALID_ARGUMENT.withDescription(
          String.format(
              "The provided client ID (\"%s\") is not a valid UUID", requestHeaders.get(asciiKey)));
    }

    // Currently, we don't check the "mongodb-client" field because it's only used for logging.
    return Status.OK;
  }

  private static void populateResponseHeaders(Metadata responseHeaders) {
    responseHeaders.put(
        GrpcMetadata.MONGODB_MAX_WIRE_VERSION_METADATA_KEY, MONGODB_REPORT_MAX_WIRE_VERSION);
  }
}
