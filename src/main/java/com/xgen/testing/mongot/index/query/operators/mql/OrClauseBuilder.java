package com.xgen.testing.mongot.index.query.operators.mql;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.OrClause;

public class OrClauseBuilder {
  private final ImmutableList.Builder<Clause> clausesBuilder = ImmutableList.builder();

  public OrClauseBuilder addClause(Clause clause) {
    this.clausesBuilder.add(clause);
    return this;
  }

  public OrClause build() {
    ImmutableList<Clause> clauses = this.clausesBuilder.build();
    checkArg(clauses.size() > 0, "Or clause must have at least 1 sub-filter.");
    return new OrClause(clauses);
  }
}
