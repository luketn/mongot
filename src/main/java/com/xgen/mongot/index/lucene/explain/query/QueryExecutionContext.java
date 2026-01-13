package com.xgen.mongot.index.lucene.explain.query;

import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

/**
 * QueryExecutionContext is responsible for creating QueryExecutionContextNodes. A
 * QueryExecutionContextNode is responsible for storing information pertinent to a single node of a
 * query.
 *
 * <p>An implementer of this class is also responsible for inferring the structure of a query as it
 * runs - though this interface does not mandate how an interface must accomplish that.
 */
public interface QueryExecutionContext<T extends QueryExecutionContextNode> {

  Optional<T> getOrCreateNode(Query query);

  void replaceNode(Query original, Query rewritten);

  /**
   * Adds the rewritten query as a child node to the original query. The child relationship type is
   * defined by the occur parameter.
   */
  void addChildNode(Query parent, Query child, BooleanClause.Occur occur);

  Optional<T> getRoot();

  RewrittenQueryExecutionContextNode rewritten() throws RewrittenQueryNodeException;
}
