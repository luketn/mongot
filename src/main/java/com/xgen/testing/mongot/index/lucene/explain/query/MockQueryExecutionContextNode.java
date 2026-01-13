package com.xgen.testing.mongot.index.lucene.explain.query;

import com.xgen.mongot.index.lucene.explain.query.QueryChildren;
import com.xgen.mongot.index.lucene.explain.query.QueryExecutionContextNode;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.util.Optional;
import org.apache.lucene.search.Query;

public class MockQueryExecutionContextNode implements QueryExecutionContextNode {
  private final Query query;
  private final ExplainTimings timings;
  private final Optional<? extends QueryChildren<QueryExecutionContextNode>> children;

  public MockQueryExecutionContextNode(Query query) {
    this(query, ExplainTimings.builder().build(), Optional.empty());
  }

  public MockQueryExecutionContextNode(
      Query query,
      ExplainTimings timings,
      Optional<? extends QueryChildren<QueryExecutionContextNode>> children) {
    this.query = query;
    this.timings = timings;
    this.children = children;
  }

  @Override
  public Query getQuery() {
    return this.query;
  }

  @Override
  public ExplainTimings getTimings() {
    return this.timings;
  }

  @Override
  public Optional<? extends QueryChildren<?>> getChildren() {
    return this.children;
  }
}
