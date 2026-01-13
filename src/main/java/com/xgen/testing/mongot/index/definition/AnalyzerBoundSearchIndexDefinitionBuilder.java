package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.util.Check;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AnalyzerBoundSearchIndexDefinitionBuilder {

  private Optional<SearchIndexDefinition> index = Optional.empty();
  private final List<OverriddenBaseAnalyzerDefinition> analyzers = new ArrayList<>();

  public static AnalyzerBoundSearchIndexDefinitionBuilder builder() {
    return new AnalyzerBoundSearchIndexDefinitionBuilder();
  }

  public AnalyzerBoundSearchIndexDefinitionBuilder index(SearchIndexDefinition index) {
    this.index = Optional.of(index);
    return this;
  }

  public AnalyzerBoundSearchIndexDefinitionBuilder analyzer(
      OverriddenBaseAnalyzerDefinition analyzer) {
    this.analyzers.add(analyzer);
    return this;
  }

  public AnalyzerBoundSearchIndexDefinition build() {
    return AnalyzerBoundSearchIndexDefinition.withRelevantOverriddenAnalyzers(
        Check.isPresent(this.index, "index"), this.analyzers);
  }
}
