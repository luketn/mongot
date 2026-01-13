package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.DefaultQuerySpec;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class DefaultQueryBuilder {
  private Optional<String> queryType = Optional.empty();

  public static DefaultQueryBuilder builder() {
    return new DefaultQueryBuilder();
  }

  public DefaultQueryBuilder queryType(String queryType) {
    this.queryType = Optional.of(queryType);
    return this;
  }

  /** Builds a DefaultQuery from a DefaultQueryBuilder. */
  public DefaultQuerySpec build() {
    Check.isPresent(this.queryType, "queryType");

    return new DefaultQuerySpec(this.queryType.get());
  }
}
