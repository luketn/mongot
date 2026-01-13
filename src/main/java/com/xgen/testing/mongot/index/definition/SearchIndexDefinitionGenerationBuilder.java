package com.xgen.testing.mongot.index.definition;

import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.definition.AnalyzerBoundSearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import com.xgen.mongot.index.definition.SearchIndexDefinitionGeneration;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.index.version.IndexFormatVersion;
import com.xgen.mongot.index.version.UserIndexVersion;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.Optional;

public class SearchIndexDefinitionGenerationBuilder {

  private Optional<AnalyzerBoundSearchIndexDefinition> definition = Optional.empty();
  private Optional<Generation> generation = Optional.empty();

  public static SearchIndexDefinitionGenerationBuilder builder() {
    return new SearchIndexDefinitionGenerationBuilder();
  }

  /** convenience for a common case. */
  public static SearchIndexDefinitionGeneration create(
      SearchIndexDefinition indexDefinition,
      Generation generation,
      List<OverriddenBaseAnalyzerDefinition> analyzers) {
    var builder = AnalyzerBoundSearchIndexDefinitionBuilder.builder().index(indexDefinition);
    analyzers.forEach(builder::analyzer);
    return builder().generation(generation).definition(builder.build()).build();
  }

  public SearchIndexDefinitionGenerationBuilder definition(
      AnalyzerBoundSearchIndexDefinition definition) {
    this.definition = Optional.of(definition);
    return this;
  }

  public SearchIndexDefinitionGenerationBuilder generation(int user, int indexFormat) {
    return generation(
        new Generation(new UserIndexVersion(user), IndexFormatVersion.create(indexFormat)));
  }

  public SearchIndexDefinitionGenerationBuilder generation(Generation generation) {
    this.generation = Optional.of(generation);
    return this;
  }

  public SearchIndexDefinitionGeneration build() {
    return new SearchIndexDefinitionGeneration(
        Check.isPresent(this.definition, "definition"),
        Check.isPresent(this.generation, "generation"));
  }
}
