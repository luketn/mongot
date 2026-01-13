package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.path.string.StringPath;
import com.xgen.mongot.index.query.operators.TermBaseOperator;
import com.xgen.mongot.index.query.operators.TermFuzzyOperator;
import com.xgen.mongot.index.query.operators.TermOperator;
import com.xgen.mongot.index.query.operators.TermPrefixOperator;
import com.xgen.mongot.index.query.operators.TermRegexOperator;
import com.xgen.mongot.index.query.operators.TermWildcardOperator;
import com.xgen.testing.mongot.index.path.string.StringPathBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class TermOperatorBuilder extends QueryOperatorBuilder<TermOperator, TermOperatorBuilder> {

  private Optional<TermOperator.FuzzyOption> fuzzy = Optional.empty();
  private Optional<Boolean> prefix = Optional.empty();
  private Optional<Boolean> regex = Optional.empty();
  private Optional<Boolean> wildcard = Optional.empty();
  private final List<StringPath> path = new ArrayList<>();

  @Override
  protected TermOperatorBuilder getBuilder() {
    return this;
  }

  public TermOperatorBuilder path(String path) {
    this.path.add(StringPathBuilder.fieldPath(path));
    return this;
  }

  public TermOperatorBuilder path(StringPath path) {
    this.path.add(path);
    return this;
  }

  public TermOperatorBuilder fuzzy(TermOperator.FuzzyOption fuzzy) {
    this.fuzzy = Optional.of(fuzzy);
    return this;
  }

  public TermOperatorBuilder prefix(boolean prefix) {
    this.prefix = Optional.of(prefix);
    return this;
  }

  public TermOperatorBuilder regex(boolean regex) {
    this.regex = Optional.of(regex);
    return this;
  }

  public TermOperatorBuilder wildcard(boolean wildcard) {
    this.wildcard = Optional.of(wildcard);
    return this;
  }

  @Override
  public TermOperator build() {
    if (this.fuzzy.isPresent()) {
      var fuzzy = this.fuzzy.get();
      return new TermFuzzyOperator(getScore(), this.path, getQuery(), fuzzy);
    }

    if (this.prefix.orElse(TermOperator.Fields.PREFIX.getDefaultValue())) {
      return new TermPrefixOperator(getScore(), this.path, getQuery());
    }

    if (this.regex.orElse(TermOperator.Fields.REGEX.getDefaultValue())) {
      return new TermRegexOperator(getScore(), this.path, getQuery());
    }

    if (this.wildcard.orElse(TermOperator.Fields.WILDCARD.getDefaultValue())) {
      return new TermWildcardOperator(getScore(), this.path, getQuery());
    }

    return new TermBaseOperator(getScore(), this.path, getQuery());
  }

  public static FuzzyOptionBuilder fuzzyBuilder() {
    return new FuzzyOptionBuilder();
  }

  public static class FuzzyOptionBuilder {

    private Optional<Integer> maxEdits = Optional.empty();
    private Optional<Integer> maxExpansions = Optional.empty();
    private Optional<Integer> prefixLength = Optional.empty();

    public FuzzyOptionBuilder maxEdits(int maxEdits) {
      this.maxEdits = Optional.of(maxEdits);
      return this;
    }

    public FuzzyOptionBuilder maxExpansions(int maxExpansions) {
      this.maxExpansions = Optional.of(maxExpansions);
      return this;
    }

    public FuzzyOptionBuilder prefixLength(int prefixLength) {
      this.prefixLength = Optional.of(prefixLength);
      return this;
    }

    /** Builds the FuzzyOption. */
    public TermOperator.FuzzyOption build() {
      return new TermOperator.FuzzyOption(
          this.maxEdits.orElse(TermOperator.FuzzyOption.Fields.MAX_EDITS.getDefaultValue()),
          this.prefixLength.orElse(TermOperator.FuzzyOption.Fields.PREFIX_LENGTH.getDefaultValue()),
          this.maxExpansions.orElse(
              TermOperator.FuzzyOption.Fields.MAX_EXPANSIONS.getDefaultValue()));
    }
  }
}
