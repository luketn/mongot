package com.xgen.mongot.index.lucene.explain.query;

import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.util.Optional;
import org.apache.lucene.search.Query;

/** One context node in the query context tree. */
public interface QueryExecutionContextNode {

  // Get the query or subquery associated with this node.
  Query getQuery();

  // Get the timings object for this node.
  ExplainTimings getTimings();

  // Get all children of this node.
  Optional<? extends QueryChildren<? extends QueryExecutionContextNode>> getChildren();
}
