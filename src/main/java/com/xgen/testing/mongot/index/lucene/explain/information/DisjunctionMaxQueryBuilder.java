package com.xgen.testing.mongot.index.lucene.explain.information;

import com.xgen.mongot.index.lucene.explain.information.DisjunctionMaxQuerySpec;
import com.xgen.mongot.index.lucene.explain.information.QueryExplainInformation;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class DisjunctionMaxQueryBuilder {
  private List<QueryExplainInformation> disjuncts;
  private Optional<Float> tieBreaker = Optional.empty();

  public static DisjunctionMaxQueryBuilder builder() {
    return new DisjunctionMaxQueryBuilder();
  }

  public DisjunctionMaxQueryBuilder disjuncts(List<QueryExplainInformation> disjuncts) {
    this.disjuncts = disjuncts;
    return this;
  }

  public DisjunctionMaxQueryBuilder tieBreaker(float tieBreaker) {
    this.tieBreaker = Optional.of(tieBreaker);
    return this;
  }

  /** Builds a DisjunctionMaxQuery from a DisjunctionMaxQueryBuilder. */
  public DisjunctionMaxQuerySpec build() {
    Check.isPresent(this.tieBreaker, "tieBreaker");
    return new DisjunctionMaxQuerySpec(this.disjuncts, this.tieBreaker.get());
  }
}
