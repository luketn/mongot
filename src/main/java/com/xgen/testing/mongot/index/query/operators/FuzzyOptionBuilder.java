package com.xgen.testing.mongot.index.query.operators;

import com.xgen.mongot.index.query.operators.FuzzyOption;
import java.util.Optional;

public class FuzzyOptionBuilder {

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

  public static FuzzyOptionBuilder builder() {
    return new FuzzyOptionBuilder();
  }

  /** Builds the FuzzyOption. */
  public FuzzyOption build() {
    return new FuzzyOption(
        this.maxEdits.orElse(FuzzyOption.Fields.MAX_EDITS.getDefaultValue()),
        this.prefixLength.orElse(FuzzyOption.Fields.PREFIX_LENGTH.getDefaultValue()),
        this.maxExpansions.orElse(FuzzyOption.Fields.MAX_EXPANSIONS.getDefaultValue()));
  }
}
