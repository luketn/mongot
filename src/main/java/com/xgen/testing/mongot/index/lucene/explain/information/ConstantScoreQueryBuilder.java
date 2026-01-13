package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.ConstantScoreQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class ConstantScoreQueryBuilder {
  private Optional<QueryExplainInformation> query = Optional.empty();

  public static ConstantScoreQueryBuilder builder() {
    return new ConstantScoreQueryBuilder();
  }

  public ConstantScoreQueryBuilder query(QueryExplainInformation query) {
    this.query = Optional.of(query);
    return this;
  }

  /** Builds a ConstantScoreQuery from a ConstantScoreQueryBuilder. */
  public ConstantScoreQuerySpec build() {
    Check.isPresent(this.query, "query");

    return new ConstantScoreQuerySpec(this.query.get());
  }
}
