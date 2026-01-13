package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.CompoundClause;
import com.xgen.mongot.index.query.operators.CompoundOperator;
import com.xgen.mongot.index.query.operators.Operator;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CompoundOperatorBuilder
    extends OperatorBuilder<CompoundOperator, CompoundOperatorBuilder> {

  private final List<Operator> filter = new ArrayList<>();
  private final List<Operator> must = new ArrayList<>();
  private final List<Operator> mustNot = new ArrayList<>();
  private final List<Operator> should = new ArrayList<>();
  private Optional<Integer> minimumShouldMatch = Optional.empty();
  private Optional<List<String>> doesNotAffect = Optional.empty();

  @Override
  protected CompoundOperatorBuilder getBuilder() {
    return this;
  }

  public <T extends Operator> CompoundOperatorBuilder filter(T clause) {
    this.filter.add(clause);
    return this;
  }

  public <T extends Operator> CompoundOperatorBuilder must(T clause) {
    this.must.add(clause);
    return this;
  }

  public <T extends Operator> CompoundOperatorBuilder mustNot(T clause) {
    this.mustNot.add(clause);
    return this;
  }

  public <T extends Operator> CompoundOperatorBuilder should(T clause) {
    this.should.add(clause);
    return this;
  }

  public CompoundOperatorBuilder minimumShouldMatch(int minimumShouldMatch) {
    this.minimumShouldMatch = Optional.of(minimumShouldMatch);
    return this;
  }

  public CompoundOperatorBuilder doesNotAffect(List<String> doesNotAffect) {
    this.doesNotAffect = Optional.of(doesNotAffect);
    return this;
  }

  public CompoundOperatorBuilder doesNotAffect(String doesNotAffect) {
    this.doesNotAffect = Optional.of(List.of(doesNotAffect));
    return this;
  }

  @Override
  public CompoundOperator build() {
    return new CompoundOperator(
        getScore(),
        Optional.of(new CompoundClause(this.filter)),
        Optional.of(new CompoundClause(this.must)),
        Optional.of(new CompoundClause(this.mustNot)),
        Optional.of(new CompoundClause(this.should)),
        this.minimumShouldMatch.orElse(CompoundOperator.Fields.DEFAULT_MINIMUM_SHOULD_MATCH),
        this.doesNotAffect);
  }
}
