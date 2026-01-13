package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.UnresolvedStringPath;
import com.xgen.mongot.index.query.operators.PhraseOperator;
import com.xgen.testing.mongot.index.path.string.UnresolvedStringPathBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class PhraseOperatorBuilder
    extends QueryOperatorBuilder<PhraseOperator, PhraseOperatorBuilder> {

  private Optional<Integer> slop = Optional.empty();
  private final List<UnresolvedStringPath> path = new ArrayList<>();
  private Optional<String> synonyms = Optional.empty();

  @Override
  protected PhraseOperatorBuilder getBuilder() {
    return this;
  }

  public PhraseOperatorBuilder slop(int slop) {
    this.slop = Optional.of(slop);
    return this;
  }

  public PhraseOperatorBuilder path(String path) {
    this.path.add(UnresolvedStringPathBuilder.fieldPath(path));
    return this;
  }

  public PhraseOperatorBuilder path(UnresolvedStringPath path) {
    this.path.add(path);
    return this;
  }

  public PhraseOperatorBuilder synonyms(String synonyms) {
    this.synonyms = Optional.of(synonyms);
    return this;
  }

  @Override
  public PhraseOperator build() {
    return new PhraseOperator(
        getScore(),
        this.path,
        getQuery(),
        this.slop.orElse(PhraseOperator.Fields.SLOP.getDefaultValue()),
        this.synonyms);
  }
}
