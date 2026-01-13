package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;

public abstract class QueryOperatorBuilder<T, B extends QueryOperatorBuilder<T, B>>
    extends OperatorBuilder<T, B> {

  private final List<String> query = new ArrayList<>();

  public B query(String query) {
    this.query.add(query);
    return getBuilder();
  }

  List<String> getQuery() {
    Check.argNotEmpty(this.query, "query");
    return this.query;
  }
}
