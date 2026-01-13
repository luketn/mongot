package com.xgen.testing.mongot.server.command.search.definition.request;

import com.xgen.mongot.server.command.search.definition.request.CursorOptionsDefinition;
import java.util.Optional;

public class CursorOptionsDefinitionBuilder {

  private Optional<Integer> docsRequested = Optional.empty();
  private Optional<Integer> batchSize = Optional.empty();
  private boolean requireSequenceTokens;

  public static CursorOptionsDefinitionBuilder builder() {
    return new CursorOptionsDefinitionBuilder();
  }

  public CursorOptionsDefinitionBuilder docsRequested(int docsRequested) {
    this.docsRequested = Optional.of(docsRequested);
    return this;
  }

  public CursorOptionsDefinitionBuilder batchSize(int batchSize) {
    this.batchSize = Optional.of(batchSize);
    return this;
  }

  public CursorOptionsDefinitionBuilder requireSequenceTokens(boolean requireSequenceTokens) {
    this.requireSequenceTokens = requireSequenceTokens;
    return this;
  }

  public CursorOptionsDefinition build() {
    return new CursorOptionsDefinition(
        this.docsRequested, this.batchSize, this.requireSequenceTokens);
  }
}
