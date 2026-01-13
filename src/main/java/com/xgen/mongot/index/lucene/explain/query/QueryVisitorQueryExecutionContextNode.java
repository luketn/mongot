package com.xgen.mongot.index.lucene.explain.query;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

public class QueryVisitorQueryExecutionContextNode implements QueryExecutionContextNode {

  public final Optional<QueryVisitorQueryExecutionContextNode> parent;
  public final Query query;
  public final ExplainTimings timings;
  public final Optional<BooleanClause.Occur> queryClauseType;

  public Optional<ChildClauses> children;

  QueryVisitorQueryExecutionContextNode(Query query) {
    this(
        query,
        Optional.empty(),
        ExplainTimings.builder().build(),
        Optional.empty(),
        Optional.empty());
  }

  // If one of occur or parent is specified, the other must be specified as well.
  QueryVisitorQueryExecutionContextNode(
      Query query, BooleanClause.Occur occur, QueryVisitorQueryExecutionContextNode parent) {
    this(
        query,
        Optional.of(occur),
        ExplainTimings.builder().build(),
        Optional.of(parent),
        Optional.empty());
  }

  @VisibleForTesting
  QueryVisitorQueryExecutionContextNode(
      Query query,
      Optional<BooleanClause.Occur> queryClauseType,
      ExplainTimings timings,
      Optional<QueryVisitorQueryExecutionContextNode> parent,
      Optional<ChildClauses> children) {
    this.query = query;
    this.queryClauseType = queryClauseType;
    this.timings = timings;
    this.parent = parent;
    this.children = children;
  }

  /** Add a child query to this node with specified occur. */
  public QueryVisitorQueryExecutionContextNode addChild(Query query, BooleanClause.Occur occur) {
    if (this.children.isEmpty()) {
      this.children = Optional.of(new ChildClauses());
    }

    var child = new QueryVisitorQueryExecutionContextNode(query, occur, this);
    this.children.get().addClause(child, occur);

    return child;
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
  public Optional<ChildClauses> getChildren() {
    return this.children;
  }

  public RewrittenQueryExecutionContextNode rewritten() throws RewrittenQueryNodeException {
    return RewrittenQueryExecutionContextNode.create(
        this.query, this.timings, this.children, this.queryClauseType);
  }

  @Override
  public String toString() {
    return this.children.map(QueryChildren::toString).orElse(this.query.toString());
  }
}
