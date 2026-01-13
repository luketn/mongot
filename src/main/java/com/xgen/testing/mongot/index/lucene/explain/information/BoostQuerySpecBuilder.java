package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.BoostQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class BoostQuerySpecBuilder {
  private Optional<QueryExplainInformation> query = Optional.empty();
  private Optional<Float> boost = Optional.empty();

  public static BoostQuerySpecBuilder builder() {
    return new BoostQuerySpecBuilder();
  }

  public BoostQuerySpecBuilder query(QueryExplainInformation query) {
    this.query = Optional.of(query);
    return this;
  }

  public BoostQuerySpecBuilder boost(float boost) {
    this.boost = Optional.of(boost);
    return this;
  }

  /** Builds a ConstantScoreQuery from a BoostQuerySpecCreator. */
  public BoostQuerySpec build() {
    Check.isPresent(this.query, "query");
    Check.isPresent(this.boost, "boost");

    return new BoostQuerySpec(this.query.get(), this.boost.get());
  }
}
