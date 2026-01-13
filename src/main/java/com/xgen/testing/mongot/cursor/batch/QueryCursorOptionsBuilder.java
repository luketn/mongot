package com.xgen.testing.mongot.cursor.batch;

import com.xgen.mongot.cursor.batch.QueryCursorOptions;
import java.util.Optional;

public class QueryCursorOptionsBuilder {

  private Optional<Integer> docsRequested = Optional.empty();
  private Optional<Integer> batchSize = Optional.empty();
  private boolean requireSequenceTokens;

  public QueryCursorOptionsBuilder docsRequested(int docsRequested) {
    this.docsRequested = Optional.of(docsRequested);
    return this;
  }

  public QueryCursorOptionsBuilder batchSize(int batchSize) {
    this.batchSize = Optional.of(batchSize);
    return this;
  }

  public QueryCursorOptionsBuilder requireSequenceTokens(boolean requireSequenceTokens) {
    this.requireSequenceTokens = requireSequenceTokens;
    return this;
  }

  public static QueryCursorOptions empty() {
    return new QueryCursorOptionsBuilder().build();
  }

  public QueryCursorOptions build() {
    return new QueryCursorOptions(this.docsRequested, this.batchSize, this.requireSequenceTokens);
  }

  public static QueryCursorOptionsBuilder builder() {
    return new QueryCursorOptionsBuilder();
  }
}
