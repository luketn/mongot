package com.xgen.testing.mongot.index.query.operators.mql;

public class ClauseBuilder {
  public static AndClauseBuilder andClause() {
    return new AndClauseBuilder();
  }

  public static OrClauseBuilder orClause() {
    return new OrClauseBuilder();
  }

  public static NorClauseBuilder norClause() {
    return new NorClauseBuilder();
  }

  public static SimpleClauseBuilder simpleClause() {
    return new SimpleClauseBuilder();
  }
}
