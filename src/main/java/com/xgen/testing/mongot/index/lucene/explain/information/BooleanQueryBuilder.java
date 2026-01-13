package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.BooleanQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import java.util.List;
import java.util.Optional;

public class BooleanQueryBuilder {
  private Optional<List<QueryExplainInformation>> must = Optional.empty();
  private Optional<List<QueryExplainInformation>> mustNot = Optional.empty();
  private Optional<List<QueryExplainInformation>> should = Optional.empty();
  private Optional<List<QueryExplainInformation>> filter = Optional.empty();
  private Optional<Integer> minimumShouldMatch = Optional.empty();

  public static BooleanQueryBuilder builder() {
    return new BooleanQueryBuilder();
  }

  public BooleanQueryBuilder must(List<QueryExplainInformation> must) {
    this.must = Optional.of(must);
    return this;
  }

  public BooleanQueryBuilder mustNot(List<QueryExplainInformation> mustNot) {
    this.mustNot = Optional.of(mustNot);
    return this;
  }

  public BooleanQueryBuilder should(List<QueryExplainInformation> should) {
    this.should = Optional.of(should);
    return this;
  }

  public BooleanQueryBuilder filter(List<QueryExplainInformation> filter) {
    this.filter = Optional.of(filter);
    return this;
  }

  public BooleanQueryBuilder minimumShouldMatch(int minimumShouldMatch) {
    this.minimumShouldMatch = Optional.of(minimumShouldMatch);
    return this;
  }

  /** Builds a BooleanQuery from a BooleanQueryBuilder. */
  public BooleanQuerySpec build() {
    return new BooleanQuerySpec(
        this.must, this.mustNot, this.should, this.filter, this.minimumShouldMatch);
  }
}
