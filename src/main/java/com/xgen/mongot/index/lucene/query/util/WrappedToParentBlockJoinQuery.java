package com.xgen.mongot.index.lucene.query.util;

import java.io.IOException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;

/**
 * {@link WrappedToParentBlockJoinQuery} extends a {@link ToParentBlockJoinQuery} and contains its
 * parentFilter and scoreMode, since the latter two are not accessible via getters in {@link
 * ToParentBlockJoinQuery} but are necessary when the child query needs to be wrapped for computing
 * score details.
 */
public class WrappedToParentBlockJoinQuery extends ToParentBlockJoinQuery {
  private final BitSetProducer parentsFilter;
  private final ScoreMode scoreMode;

  public WrappedToParentBlockJoinQuery(
      Query childQuery, BitSetProducer parentsFilter, ScoreMode scoreMode) {
    super(childQuery, parentsFilter, scoreMode);
    this.parentsFilter = parentsFilter;
    this.scoreMode = scoreMode;
  }

  public WrappedToParentBlockJoinQuery withWrappedChildQuery(Query query) {
    return new WrappedToParentBlockJoinQuery(query, this.parentsFilter, this.scoreMode);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    getChildQuery().visit(visitor.getSubVisitor(BooleanClause.Occur.MUST, this));
  }

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    Query rewrittenChildQuery = this.getChildQuery().rewrite(searcher);
    if (rewrittenChildQuery == this.getChildQuery()) {
      return this;
    }
    return withWrappedChildQuery(rewrittenChildQuery);
  }
}
