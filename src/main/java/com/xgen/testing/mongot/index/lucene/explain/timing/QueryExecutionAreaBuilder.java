package com.xgen.testing.mongot.index.lucene.explain.timing;

import com.xgen.mongot.index.lucene.explain.timing.QueryExecutionArea;
import com.xgen.mongot.util.Check;
import java.util.Map;
import java.util.Optional;

public class QueryExecutionAreaBuilder {
  private Optional<Long> nanosElapsed;
  private Optional<Map<String, Long>> invocationCounts = Optional.empty();

  public static QueryExecutionAreaBuilder builder() {
    return new QueryExecutionAreaBuilder();
  }

  public QueryExecutionAreaBuilder nanosElapsed(long nanosElapsed) {
    this.nanosElapsed = Optional.of(nanosElapsed);
    return this;
  }

  public QueryExecutionAreaBuilder invocationCounts(Map<String, Long> invocationCounts) {
    this.invocationCounts = Optional.of(invocationCounts);
    return this;
  }

  /** Builds a QueryExecutionArea from the QueryExecutionAreaBuilder. */
  public QueryExecutionArea build() {
    Check.isPresent(this.nanosElapsed, "nanosElapsed");

    return new QueryExecutionArea(this.nanosElapsed.get(), this.invocationCounts);
  }
}
