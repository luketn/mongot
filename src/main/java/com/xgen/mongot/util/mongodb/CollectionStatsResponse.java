package com.xgen.mongot.util.mongodb;

import static com.xgen.mongot.util.Check.checkState;

import com.xgen.mongot.util.mongodb.serialization.CollectionStatsResponseProxy;

public class CollectionStatsResponse {

  private final long count;
  private final int avgObjSize;

  public CollectionStatsResponse(long count, int avgObjSize) {
    this.count = count;
    this.avgObjSize = avgObjSize;
  }

  /** Constructs a new CollectionStatsResponse from the given CollectionStatsResponseProxy. */
  public static CollectionStatsResponse fromProxy(CollectionStatsResponseProxy proxy) {
    checkState(
        proxy.getOk() == 1.0,
        "CollectionStatsResponseProxy had unexpected ok field value: %s",
        proxy.getOk());
    return new CollectionStatsResponse(proxy.getCount(), proxy.getAvgObjSize());
  }

  /** Returns approximate count based on collection metadata. */
  public long getCount() {
    return this.count;
  }

  public int getAvgObjSize() {
    return this.avgObjSize;
  }
}
