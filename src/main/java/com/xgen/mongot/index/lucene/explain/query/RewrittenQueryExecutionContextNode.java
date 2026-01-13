package com.xgen.mongot.index.lucene.explain.query;

import com.google.common.annotations.VisibleForTesting;
import com.xgen.mongot.index.lucene.explain.timing.ExplainTimings;
import com.xgen.mongot.util.Optionals;
import java.util.Objects;
import java.util.Optional;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Query;

/**
 * Instances of this class must be constructed only after a Lucene query has fully finished being
 * rewritten. Note that this class defines <code>equals/hashCode</code> methods which do <b>not</b>
 * check equality of the ExplainTimings, and the class does not maintain references to parent <code>
 * QueryExecutionContextNode</code>'s as it is unnecessary post query execution.
 *
 * <p>We depend on the <code>BooleanClause.Occur</code> and the <code>Query</code> objects for
 * equality testing which is necessary to check that 2 explain query trees are structurally equal
 * before attempting to merge statistics via the <code>mergeTrees()</code> function.
 */
public class RewrittenQueryExecutionContextNode implements QueryExecutionContextNode {

  private final Query query;
  private final ExplainTimings timings;
  private final Optional<RewrittenChildClauses> children;
  private final Optional<BooleanClause.Occur> occur;

  @VisibleForTesting
  public RewrittenQueryExecutionContextNode(
      Query query,
      ExplainTimings timings,
      Optional<RewrittenChildClauses> children,
      Optional<BooleanClause.Occur> occur) {
    this.query = query;
    this.timings = timings;
    this.children = children;
    this.occur = occur;
  }

  static RewrittenQueryExecutionContextNode create(
      Query query,
      ExplainTimings timings,
      Optional<ChildClauses> children,
      Optional<BooleanClause.Occur> queryClauseType)
      throws RewrittenQueryNodeException {
    return new RewrittenQueryExecutionContextNode(
        query,
        timings,
        Optionals.mapOrThrowChecked(children, ChildClauses::rewritten),
        queryClauseType);
  }

  public static RewrittenQueryExecutionContextNode mergeTrees(
      RewrittenQueryExecutionContextNode first, RewrittenQueryExecutionContextNode second)
      throws RewrittenQueryNodeException {
    RewrittenQueryNodeException.checkNodesAreEqual(first, second);

    ExplainTimings mergedTimings = ExplainTimings.merge(first.getTimings(), second.getTimings());

    Optional<RewrittenChildClauses> mergedChildren =
        RewrittenChildClauses.mergeClauses(first.children, second.children);
    return new RewrittenQueryExecutionContextNode(
        first.getQuery(), mergedTimings, mergedChildren, first.occur);
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
  public Optional<RewrittenChildClauses> getChildren() {
    return this.children;
  }

  public Optional<BooleanClause.Occur> getOccur() {
    return this.occur;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RewrittenQueryExecutionContextNode that)) {
      return false;
    }
    return Objects.equals(this.query, that.query)
        && Objects.equals(this.children, that.children)
        && Objects.equals(this.occur, that.occur);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.query, this.children, this.occur);
  }

  @Override
  public String toString() {
    return this.children.map(QueryChildren::toString).orElse(this.query.toString());
  }
}
