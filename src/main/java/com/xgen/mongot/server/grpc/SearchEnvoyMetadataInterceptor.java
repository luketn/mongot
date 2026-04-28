package com.xgen.mongot.server.grpc;

import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import com.xgen.mongot.trace.Tracing;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import java.util.Optional;

public class SearchEnvoyMetadataInterceptor implements ServerInterceptor {

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    try (var interceptorSpan =
        Tracing.detailedSpanGuard("mongot.grpc.search_envoy_metadata_interceptor")) {
      interceptorSpan
          .getSpan()
          .setAttribute("rpc.method", fullMethodName(call));

      Optional<SearchEnvoyMetadata> searchEnvoyMetadata =
          Optional.ofNullable(headers.get(GrpcMetadata.SEARCH_ENVOY_METADATA_KEY));
      interceptorSpan
          .getSpan()
          .setAttribute(
              "mongot.grpc.search_envoy_metadata.present", searchEnvoyMetadata.isPresent());
      Context context =
          Context.current()
              .withValue(
                  GrpcContext.SEARCH_ENVOY_METADATA_KEY,
                  searchEnvoyMetadata.orElse(SearchEnvoyMetadata.getDefaultInstance()));

      return Contexts.interceptCall(context, call, headers, next);
    }
  }

  private static <ReqT, RespT> String fullMethodName(ServerCall<ReqT, RespT> call) {
    return call.getMethodDescriptor() == null
        ? "unknown"
        : call.getMethodDescriptor().getFullMethodName();
  }
}
