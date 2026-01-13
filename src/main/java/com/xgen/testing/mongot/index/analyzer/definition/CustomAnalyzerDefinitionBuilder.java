package com.xgen.testing.mongot.index.analyzer.definition;

import com.xgen.mongot.index.analyzer.custom.CharFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenizerDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class CustomAnalyzerDefinitionBuilder {
  private final String name;
  private final TokenizerDefinition tokenizerDefinition;
  private final List<CharFilterDefinition> charFilterDefinitions = new ArrayList<>();
  private final List<TokenFilterDefinition> tokenFilterDefinitions = new ArrayList<>();

  public CustomAnalyzerDefinitionBuilder(String name, TokenizerDefinition tokenizerDefinition) {
    this.name = name;
    this.tokenizerDefinition = tokenizerDefinition;
  }

  public static CustomAnalyzerDefinitionBuilder builder(
      String name, TokenizerDefinition tokenizerDefinition) {
    return new CustomAnalyzerDefinitionBuilder(name, tokenizerDefinition);
  }

  /** Add a charFilter to the CustomAnalyzerDefinition. */
  public CustomAnalyzerDefinitionBuilder charFilter(CharFilterDefinition charFilter) {
    this.charFilterDefinitions.add(charFilter);
    return this;
  }

  /** Adds charFilters to those to be used by this CustomAnalyzerDefinition. */
  public CustomAnalyzerDefinitionBuilder charFilters(List<CharFilterDefinition> charFilters) {
    this.charFilterDefinitions.addAll(charFilters);
    return this;
  }

  /** Add a tokenFilter to this CustomAnalyzerDefinition. */
  public CustomAnalyzerDefinitionBuilder tokenFilter(TokenFilterDefinition tokenFilter) {
    this.tokenFilterDefinitions.add(tokenFilter);
    return this;
  }

  /** Adds tokenFilters to those to be used by this CustomAnalyzerDefinition. */
  public CustomAnalyzerDefinitionBuilder tokenFilters(List<TokenFilterDefinition> tokenFilters) {
    this.tokenFilterDefinitions.addAll(tokenFilters);
    return this;
  }

  /** Build a CustomAnalyzerDefinition from this builder. */
  public CustomAnalyzerDefinition build() {
    return new CustomAnalyzerDefinition(
        this.name,
        this.charFilterDefinitions.isEmpty()
            ? Optional.empty()
            : Optional.of(this.charFilterDefinitions),
        this.tokenizerDefinition,
        this.tokenFilterDefinitions.isEmpty()
            ? Optional.empty()
            : Optional.of(this.tokenFilterDefinitions));
  }
}
