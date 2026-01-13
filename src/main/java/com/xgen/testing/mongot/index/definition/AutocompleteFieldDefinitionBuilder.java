package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.SimilarityDefinition;
import java.util.Optional;

public class AutocompleteFieldDefinitionBuilder {
  private Optional<AutocompleteFieldDefinition.TokenizationStrategy> tokenizationStrategy =
      Optional.empty();
  private Optional<Boolean> foldDiacritics = Optional.empty();
  private Optional<Integer> minGrams = Optional.empty();
  private Optional<Integer> maxGrams = Optional.empty();
  private Optional<String> analyzer = Optional.empty();
  private Optional<SimilarityDefinition> similarity = Optional.empty();

  public static AutocompleteFieldDefinitionBuilder builder() {
    return new AutocompleteFieldDefinitionBuilder();
  }

  public AutocompleteFieldDefinitionBuilder tokenizationStrategy(
      AutocompleteFieldDefinition.TokenizationStrategy tokenizationStrategy) {
    this.tokenizationStrategy = Optional.of(tokenizationStrategy);
    return this;
  }

  public AutocompleteFieldDefinitionBuilder foldDiacritics(Boolean foldDiacritics) {
    this.foldDiacritics = Optional.of(foldDiacritics);
    return this;
  }

  public AutocompleteFieldDefinitionBuilder minGrams(Integer minGrams) {
    this.minGrams = Optional.of(minGrams);
    return this;
  }

  public AutocompleteFieldDefinitionBuilder maxGrams(Integer maxGrams) {
    this.maxGrams = Optional.of(maxGrams);
    return this;
  }

  public AutocompleteFieldDefinitionBuilder analyzer(String analyzer) {
    this.analyzer = Optional.of(analyzer);
    return this;
  }

  public AutocompleteFieldDefinitionBuilder similarity(SimilarityDefinition similarity) {
    this.similarity = Optional.of(similarity);
    return this;
  }

  public AutocompleteFieldDefinition build() {
    return new AutocompleteFieldDefinition(
        this.minGrams.orElse(AutocompleteFieldDefinition.Fields.MIN_GRAMS.getDefaultValue()),
        this.maxGrams.orElse(AutocompleteFieldDefinition.Fields.MAX_GRAMS.getDefaultValue()),
        this.foldDiacritics.orElse(
            AutocompleteFieldDefinition.Fields.FOLD_DIACRITICS.getDefaultValue()),
        this.tokenizationStrategy.orElse(
            AutocompleteFieldDefinition.Fields.TOKENIZATION_STRATEGY.getDefaultValue()),
        this.analyzer,
        this.similarity);
  }
}
