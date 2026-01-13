package com.xgen.testing.mongot.index.query.operators.mql;

import static com.xgen.mongot.util.Check.checkArg;

import com.google.common.collect.ImmutableList;
import com.xgen.mongot.index.query.operators.mql.AndClause;
import com.xgen.mongot.index.query.operators.mql.Clause;
import com.xgen.mongot.util.Check;
import java.util.Optional;

public class AndClauseBuilder {
  private final ImmutableList.Builder<Clause> clausesBuilder = ImmutableList.builder();
  private Optional<Boolean> explicitAnd;

  public AndClauseBuilder addClause(Clause clause) {
    this.clausesBuilder.add(clause);
    return this;
  }

  public AndClauseBuilder explicitAnd(boolean explicitAnd) {
    this.explicitAnd = Optional.of(explicitAnd);
    return this;
  }

  public AndClause build() {
    Check.isPresent(this.explicitAnd, "explicitAnd");
    ImmutableList<Clause> clauses = this.clausesBuilder.build();
    if (this.explicitAnd.get()) {
      checkArg(clauses.size() > 0, "And clause must have at least 1 sub-filter.");
    }
    return new AndClause(clauses, this.explicitAnd.get());
  }
}
