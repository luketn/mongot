package com.xgen.testing.mongot.index.lucene.explain.query;

import com.xgen.mongot.index.lucene.explain.query.RewrittenChildClauses;
import com.xgen.mongot.index.lucene.explain.query.RewrittenQueryExecutionContextNode;
import com.xgen.mongot.util.Check;
import java.util.HashSet;
import java.util.Set;

public class RewrittenChildClausesBuilder {
  private final Set<RewrittenQueryExecutionContextNode> mustChildren = new HashSet<>();
  private final Set<RewrittenQueryExecutionContextNode> mustNotChildren = new HashSet<>();
  private final Set<RewrittenQueryExecutionContextNode> shouldChildren = new HashSet<>();
  private final Set<RewrittenQueryExecutionContextNode> filterChildren = new HashSet<>();

  public static RewrittenChildClausesBuilder builder() {
    return new RewrittenChildClausesBuilder();
  }

  public RewrittenChildClausesBuilder child(RewrittenQueryExecutionContextNode child) {
    switch (Check.isPresent(child.getOccur(), "occur")) {
      case MUST -> {
        this.mustChildren.add(child);
      }
      case FILTER -> {
        this.filterChildren.add(child);
      }
      case SHOULD -> {
        this.shouldChildren.add(child);
      }
      case MUST_NOT -> {
        this.mustNotChildren.add(child);
      }
    }

    return this;
  }

  public RewrittenChildClauses build() {
    return new RewrittenChildClauses(
        this.mustChildren, this.mustNotChildren, this.shouldChildren, this.filterChildren);
  }
}
