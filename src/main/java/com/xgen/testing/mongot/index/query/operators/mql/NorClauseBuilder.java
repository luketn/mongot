package com.xgen.testing.mongot.index.query.operators.mql;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.index.query.operators.mql.NorClause;

public class NorClauseBuilder {
  private final ImmutableList.Builder<Clause> clausesBuilder = ImmutableList.builder();

  public NorClauseBuilder addClause(Clause clause) {
    this.clausesBuilder.add(clause);
    return this;
  }

  public NorClause build() {
    ImmutableList<Clause> clauses = this.clausesBuilder.build();
    checkArg(clauses.size() > 0, "Nor clause must have at least 1 sub-filter.");
    return new NorClause(clauses);
  }
}
