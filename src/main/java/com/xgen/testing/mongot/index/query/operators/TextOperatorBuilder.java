package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.query.operators.FuzzyOption;
import com.xgen.mongot.index.query.operators.TextOperator;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TextOperatorBuilder extends QueryOperatorBuilder<TextOperator, TextOperatorBuilder> {

  private final List<UnresolvedStringPath> path = new ArrayList<>();
  private Optional<FuzzyOption> fuzzy = Optional.empty();
  private Optional<String> synonyms = Optional.empty();
  private Optional<TextOperator.MatchCriteria> matchCriteria = Optional.empty();

  @Override
  protected TextOperatorBuilder getBuilder() {
    return this;
  }

  public TextOperatorBuilder path(String path) {
    this.path.add(UnresolvedStringPathBuilder.fieldPath(path));
    return this;
  }

  public TextOperatorBuilder path(UnresolvedStringPath path) {
    this.path.add(path);
    return this;
  }

  public TextOperatorBuilder fuzzy(FuzzyOption fuzzy) {
    this.fuzzy = Optional.of(fuzzy);
    return this;
  }

  public TextOperatorBuilder multi(String path, String multi) {
    this.path.add(UnresolvedStringPathBuilder.withMulti(path, multi));
    return this;
  }

  public TextOperatorBuilder matchCriteria(TextOperator.MatchCriteria matchCriteria) {
    this.matchCriteria = Optional.of(matchCriteria);
    return this;
  }

  public TextOperatorBuilder synonyms(String synonyms) {
    this.synonyms = Optional.of(synonyms);
    return this;
  }

  @Override
  public TextOperator build() {
    return new TextOperator(
        getScore(), this.path, getQuery(), this.fuzzy, this.synonyms, this.matchCriteria);
  }

  public static FuzzyOptionBuilder fuzzyBuilder() {
    return new FuzzyOptionBuilder();
  }
}
