package com.xgen.testing.mongot.index.lucene.query;

import com.xgen.mongot.index.lucene.query.ScoreDetailsWrappedQuery;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import org.apache.lucene.search.Query;

public class ScoreDetailsWrappedQueryBuilder {
  private Optional<Query> query = Optional.empty();

  public static ScoreDetailsWrappedQueryBuilder builder() {
    return new ScoreDetailsWrappedQueryBuilder();
  }

  public ScoreDetailsWrappedQueryBuilder query(Query query) {
    this.query = Optional.of(query);
    return this;
  }

  public ScoreDetailsWrappedQuery build() {
    Check.isPresent(this.query, "query");
    return new ScoreDetailsWrappedQuery(this.query.get());
  }
}
