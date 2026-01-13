package com.xgen.mongot.index.lucene.query.util;

import java.io.IOException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ToChildBlockJoinQuery;

/**
 * {@link WrappedToChildBlockJoinQuery} extends a {@link ToChildBlockJoinQuery} and contains its
 * parentFilter and ancestorBitSetProducer. The goal of this class is to support recursive
 * explaining for {@link ToChildBlockJoinQuery#parentQuery} by overriding {@link
 * ToChildBlockJoinQuery#visit(QueryVisitor)}.
 */
public class WrappedToChildBlockJoinQuery extends ToChildBlockJoinQuery {
  private final BitSetProducer parentsFilter;

  public WrappedToChildBlockJoinQuery(Query parentQuery, BitSetProducer parentsFilter) {
    super(parentQuery, parentsFilter);
    this.parentsFilter = parentsFilter;
  }

  public WrappedToChildBlockJoinQuery withWrappedParentQuery(Query query) {
    return new WrappedToChildBlockJoinQuery(query, this.parentsFilter);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    getParentQuery().visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    Query rewrittenParentQuery = this.getParentQuery().rewrite(searcher);
    if (rewrittenParentQuery == this.getParentQuery()) {
      return this;
    }
    return withWrappedParentQuery(rewrittenParentQuery);
  }
}
