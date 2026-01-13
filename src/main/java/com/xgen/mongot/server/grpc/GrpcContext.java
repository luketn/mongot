package com.xgen.mongot.server.grpc;

import com.xgen.mongot.searchenvoy.grpc.SearchEnvoyMetadata;
import io.grpc.Context;

public class GrpcContext {
  public static final Context.Key<SearchEnvoyMetadata> SEARCH_ENVOY_METADATA_KEY =
      Context.key("search-envoy-metadata");
}
