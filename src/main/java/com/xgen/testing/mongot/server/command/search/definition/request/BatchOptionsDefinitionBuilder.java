package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.server.command.search.definition.request.BatchOptionsDefinition;
import java.util.Optional;

public class BatchOptionsDefinitionBuilder {

  private Optional<Integer> docsRequested = Optional.empty();
  private Optional<Integer> batchSize = Optional.empty();

  public static BatchOptionsDefinitionBuilder builder() {
    return new BatchOptionsDefinitionBuilder();
  }

  public BatchOptionsDefinitionBuilder docsRequested(int docsRequested) {
    this.docsRequested = Optional.of(docsRequested);
    return this;
  }

  public BatchOptionsDefinitionBuilder batchSize(int batchSize) {
    this.batchSize = Optional.of(batchSize);
    return this;
  }

  public BatchOptionsDefinition build() {
    return new BatchOptionsDefinition(this.docsRequested, this.batchSize);
  }
}
