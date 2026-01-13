package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.operators.SearchDisjunctionOperator;
import com.xgen.mongot.index.query.operators.SearchOperator;
import com.xgen.mongot.index.query.operators.SearchPhraseOperator;
import com.xgen.mongot.index.query.operators.SearchPhrasePrefixOperator;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SearchOperatorBuilder
    extends QueryOperatorBuilder<SearchOperator, SearchOperatorBuilder> {

  private Optional<SearchOperator.PhraseOption> phrase = Optional.empty();
  private final List<StringPath> path = new ArrayList<>();

  @Override
  protected SearchOperatorBuilder getBuilder() {
    return this;
  }

  public SearchOperatorBuilder phrase(SearchOperator.PhraseOption phrase) {
    this.phrase = Optional.of(phrase);
    return this;
  }

  public SearchOperatorBuilder path(String path) {
    this.path.add(StringPathBuilder.fieldPath(path));
    return this;
  }

  public SearchOperatorBuilder path(StringPath path) {
    this.path.add(path);
    return this;
  }

  @Override
  public SearchOperator build() {
    var baseOperator = new SearchDisjunctionOperator(getScore(), this.path, getQuery());
    if (this.phrase.isEmpty()) {
      return baseOperator;
    }

    var phrase = this.phrase.get();
    if (phrase.prefix()) {
      return new SearchPhrasePrefixOperator(baseOperator, phrase);
    }

    return new SearchPhraseOperator(baseOperator, phrase);
  }

  public static PhraseOptionBuilder phraseBuilder() {
    return new PhraseOptionBuilder();
  }

  public static class PhraseOptionBuilder {

    private Optional<Integer> maxExpansions = Optional.empty();
    private Optional<Boolean> prefix = Optional.empty();
    private Optional<Integer> slop = Optional.empty();

    public PhraseOptionBuilder maxExpansions(int maxExpansions) {
      this.maxExpansions = Optional.of(maxExpansions);
      return this;
    }

    public PhraseOptionBuilder prefix(boolean prefix) {
      this.prefix = Optional.of(prefix);
      return this;
    }

    public PhraseOptionBuilder slop(int slop) {
      this.slop = Optional.of(slop);
      return this;
    }

    /** Builds the PhraseOption. */
    public SearchOperator.PhraseOption build() {
      return new SearchOperator.PhraseOption(
          this.maxExpansions.orElse(
              SearchOperator.PhraseOption.Fields.MAX_EXPANSIONS.getDefaultValue()),
          this.prefix.orElse(SearchOperator.PhraseOption.Fields.PREFIX.getDefaultValue()),
          this.slop.orElse(SearchOperator.PhraseOption.Fields.SLOP.getDefaultValue()));
    }
  }
}
