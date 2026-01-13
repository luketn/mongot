package com.xgen.mongot.util.mongodb;

import com.xgen.mongot.util.Check;
import com.xgen.mongot.util.mongodb.serialization.GetMoreCommandProxy;
import java.util.Optional;

public class GetMoreCommand {

  private final long cursorId;
  private final String collection;
  private final Optional<Integer> batchSize;
  private final Optional<Integer> maxTimeMs;

  /** Constructs a new GetMoreCommandProxy. */
  public GetMoreCommand(
      long cursorId, String collection, Optional<Integer> batchSize, Optional<Integer> maxTimeMs) {
    Check.argNotNegative(cursorId, "cursorId");

    this.cursorId = cursorId;
    this.collection = collection;
    this.batchSize = batchSize;
    this.maxTimeMs = maxTimeMs;
  }

  /** Returns a GetMoreCommandProxy for the GetMoreCommand. */
  public GetMoreCommandProxy toProxy() {
    return new GetMoreCommandProxy(this.cursorId, this.collection, this.batchSize, this.maxTimeMs);
  }
}
