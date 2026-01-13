package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.QueryStringOperator;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class QueryStringOperatorBuilder
    extends OperatorBuilder<QueryStringOperator, QueryStringOperatorBuilder> {

  private Optional<String> defaultPath = Optional.empty();
  private Optional<String> query = Optional.empty();

  @Override
  protected QueryStringOperatorBuilder getBuilder() {
    return this;
  }

  public QueryStringOperatorBuilder defaultPath(String defaultPath) {
    this.defaultPath = Optional.of(defaultPath);
    return this;
  }

  public QueryStringOperatorBuilder query(String query) {
    this.query = Optional.of(query);
    return this;
  }

  @Override
  public QueryStringOperator build() {
    Check.isPresent(this.defaultPath, "defaultPath");
    Check.isPresent(this.query, "query");

    return new QueryStringOperator(getScore(), this.query.get(), this.defaultPath.get());
  }
}
