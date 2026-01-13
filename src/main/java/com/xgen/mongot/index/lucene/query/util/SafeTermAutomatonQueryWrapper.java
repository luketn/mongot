package com.xgen.mongot.index.lucene.query.util;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.sandbox.search.TermAutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * {@link SafeTermAutomatonQueryWrapper} exists to prevent over-aggressive rewriting and
 * optimization from calling {@link TermAutomatonQuery#createWeight(IndexSearcher, ScoreMode,
 * float)} with a {@link ScoreMode} of {@link ScoreMode#COMPLETE_NO_SCORES}.
 *
 * <p>{@link TermAutomatonQuery} must call {@link TermAutomatonQuery#createWeight(IndexSearcher,
 * ScoreMode, float)} with a {@link ScoreMode} that provides document frequencies; executing a
 * {@link TermAutomatonQuery} without scoring statistics results in an {@link
 * IllegalStateException}, and may introduce issues as experienced in <a
 * href="https://jira.mongodb.org/browse/CLOUDP-101666">CLOUDP-101666</a>.
 *
 * <p>This class delegates to the wrapped {@link TermAutomatonQuery} whenever possible. On rewrite,
 * this class only continues to wrap the rewritten inner query if the result of {@code
 * query.rewrite()} is also a {@link TermAutomatonQuery}.
 */
public class SafeTermAutomatonQueryWrapper extends Query {
  private final TermAutomatonQuery query;

  SafeTermAutomatonQueryWrapper(TermAutomatonQuery query) {
    this.query = query;
  }

  public static SafeTermAutomatonQueryWrapper create(TermAutomatonQuery query) {
    return new SafeTermAutomatonQueryWrapper(query);
  }

  /**
   * Rewrite this query. If the wrapped query is rewritten to a non-TermAutomatonQuery, return that
   * query and do away with this wrapper.
   */
  @Override
  public Query rewrite(IndexSearcher reader) throws IOException {
    Query rewritten = this.query.rewrite(reader);
    if (rewritten == this.query) {
      return this;
    }

    if (rewritten instanceof TermAutomatonQuery) {
      return new SafeTermAutomatonQueryWrapper((TermAutomatonQuery) rewritten);
    }

    return rewritten;
  }

  /**
   * Delegate to the wrapped query, ensuring the ScoreMode passed is one that returns true from
   * {@link ScoreMode#needsScores()}.
   */
  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    return this.query.createWeight(searcher, leastDemandingScoreMode(scoreMode), boost);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    this.query.visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  private static ScoreMode leastDemandingScoreMode(ScoreMode scoreMode) {
    return switch (scoreMode) {
      case COMPLETE, COMPLETE_NO_SCORES -> ScoreMode.COMPLETE;
      case TOP_SCORES -> ScoreMode.TOP_SCORES;
      case TOP_DOCS, TOP_DOCS_WITH_SCORES -> ScoreMode.TOP_DOCS_WITH_SCORES;
    };
  }

  @Override
  public String toString(String field) {
    return this.query.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SafeTermAutomatonQueryWrapper that = (SafeTermAutomatonQueryWrapper) o;
    return this.query.equals(that.query);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.query);
  }
}
