package com.xgen.mongot.server.grpc;

import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
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

    Optional<SearchEnvoyMetadata> searchEnvoyMetadata =
        Optional.ofNullable(headers.get(GrpcMetadata.SEARCH_ENVOY_METADATA_KEY));
    Context context =
        Context.current()
            .withValue(
                GrpcContext.SEARCH_ENVOY_METADATA_KEY,
                searchEnvoyMetadata.orElse(SearchEnvoyMetadata.getDefaultInstance()));

    return Contexts.interceptCall(context, call, headers, next);
  }
}
