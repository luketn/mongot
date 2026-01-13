package com.xgen.testing.mongot.index.analyzer.definition;

import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.util.Check;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class OverriddenBaseAnalyzerDefinitionBuilder {

  private Optional<String> name = Optional.empty();
  private Optional<String> baseAnalyzerName = Optional.empty();
  private Optional<Boolean> ignoreCase = Optional.empty();
  private Optional<Integer> maxTokenLength = Optional.empty();
  private Optional<Set<String>> stopwords = Optional.empty();
  private Optional<Set<String>> stemExclusionSet = Optional.empty();

  public static OverriddenBaseAnalyzerDefinitionBuilder builder() {
    return new OverriddenBaseAnalyzerDefinitionBuilder();
  }

  public OverriddenBaseAnalyzerDefinitionBuilder name(String name) {
    this.name = Optional.of(name);
    return this;
  }

  public OverriddenBaseAnalyzerDefinitionBuilder baseAnalyzerName(String baseAnalyzerName) {
    this.baseAnalyzerName = Optional.of(baseAnalyzerName);
    return this;
  }

  public OverriddenBaseAnalyzerDefinitionBuilder ignoreCase(boolean ignoreCase) {
    this.ignoreCase = Optional.of(ignoreCase);
    return this;
  }

  public OverriddenBaseAnalyzerDefinitionBuilder maxTokenLength(int maxTokenLength) {
    this.maxTokenLength = Optional.of(maxTokenLength);
    return this;
  }

  /** Add a stopword to the AnalyzerDefinition. */
  public OverriddenBaseAnalyzerDefinitionBuilder stopword(String stopword) {
    this.stopwords = Optional.of(this.stopwords.orElseGet(HashSet::new));
    this.stopwords.get().add(stopword);
    return this;
  }

  /** Add stopwords to the AnalyzerDefinition. */
  public OverriddenBaseAnalyzerDefinitionBuilder stopwords(Set<String> stopwords) {
    this.stopwords = Optional.of(this.stopwords.orElseGet(HashSet::new));
    this.stopwords.get().addAll(stopwords);
    return this;
  }

  /** Add a stem to exclude to the AnalyzerDefinition. */
  public OverriddenBaseAnalyzerDefinitionBuilder excludeStem(String stem) {
    this.stemExclusionSet = Optional.of(this.stemExclusionSet.orElseGet(HashSet::new));
    this.stemExclusionSet.get().add(stem);
    return this;
  }

  /** Add stems to exclude to the AnalyzerDefinition. */
  public OverriddenBaseAnalyzerDefinitionBuilder excludeStems(Set<String> stems) {
    this.stemExclusionSet = Optional.of(this.stemExclusionSet.orElseGet(HashSet::new));
    this.stemExclusionSet.get().addAll(stems);
    return this;
  }

  /** Build the AnalyzerDefinition. */
  public OverriddenBaseAnalyzerDefinition build() {
    return new OverriddenBaseAnalyzerDefinition(
        Check.isPresent(this.name, "name"),
        Check.isPresent(this.baseAnalyzerName, "baseAnalyzerName"),
        this.ignoreCase.orElse(
            OverriddenBaseAnalyzerDefinition.Fields.IGNORE_CASE.getDefaultValue()),
        this.maxTokenLength,
        this.stopwords,
        this.stemExclusionSet);
  }
}
