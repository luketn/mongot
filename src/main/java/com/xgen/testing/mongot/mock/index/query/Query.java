package com.xgen.testing.mongot.mock.index.query;

import com.xgen.testing.mongot.index.query.OperatorQueryBuilder;
import com.xgen.testing.mongot.index.query.operators.OperatorBuilder;

public class Query {

  /** Returns a QueryDefinition that can be used in tests that require one. */
  public static com.xgen.mongot.index.query.OperatorQuery mockQuery() {
    return OperatorQueryBuilder.builder()
        .index(com.xgen.mongot.index.query.Query.Fields.INDEX.getDefaultValue())
        .operator(OperatorBuilder.term().path("title").query("godfather").build())
        .returnStoredSource(false)
        .build();
  }

  /** Returns a minimal valid OperatorQueryBuilder that can be modified for tests. */
  public static OperatorQueryBuilder simpleQueryBuilder() {
    return OperatorQueryBuilder.builder()
        .index(com.xgen.mongot.index.query.Query.Fields.INDEX.getDefaultValue())
        .operator(OperatorBuilder.term().path("title").query("godfather").build())
        .returnStoredSource(false);
  }

  /** Returns a query definition that has a failing index name. */
  public static com.xgen.mongot.index.query.OperatorQuery mockBadIndexNameQuery() {
    return OperatorQueryBuilder.builder()
        .index("BAD NAME")
        .operator(OperatorBuilder.term().path("title").query("godfather").build())
        .returnStoredSource(false)
        .build();
  }
}
