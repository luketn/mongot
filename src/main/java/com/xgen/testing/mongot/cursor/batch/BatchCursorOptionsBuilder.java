package com.xgen.testing.mongot.cursor.batch;

import com.xgen.mongot.cursor.batch.BatchCursorOptions;
import java.util.Optional;

public class BatchCursorOptionsBuilder {

  private Optional<Integer> docsRequested = Optional.empty();
  private Optional<Integer> batchSize = Optional.empty();

  public BatchCursorOptionsBuilder docsRequested(int docsRequested) {
    this.docsRequested = Optional.of(docsRequested);
    return this;
  }

  public BatchCursorOptionsBuilder batchSize(int batchSize) {
    this.batchSize = Optional.of(batchSize);
    return this;
  }

  public static BatchCursorOptions empty() {
    return BatchCursorOptions.empty();
  }

  public BatchCursorOptions build() {
    return new BatchCursorOptions(this.docsRequested, this.batchSize);
  }

  public static BatchCursorOptionsBuilder builder() {
    return new BatchCursorOptionsBuilder();
  }
}
