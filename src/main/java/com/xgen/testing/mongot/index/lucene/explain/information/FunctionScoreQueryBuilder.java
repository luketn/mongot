package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.FunctionScoreQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class FunctionScoreQueryBuilder {
  private Optional<String> scoreFunction = Optional.empty();
  private Optional<QueryExplainInformation> query = Optional.empty();

  public static FunctionScoreQueryBuilder builder() {
    return new FunctionScoreQueryBuilder();
  }

  public FunctionScoreQueryBuilder scoreFunction(String scoreFunction) {
    this.scoreFunction = Optional.of(scoreFunction);
    return this;
  }

  public FunctionScoreQueryBuilder query(QueryExplainInformation query) {
    this.query = Optional.of(query);
    return this;
  }

  /** Builds a FunctionScoreQuery from a FunctionScoreQueryBuilder. */
  public FunctionScoreQuerySpec build() {
    Check.isPresent(this.scoreFunction, "scoreFunction");
    Check.isPresent(this.query, "query");

    return new FunctionScoreQuerySpec(this.scoreFunction.get(), this.query.get());
  }
}
