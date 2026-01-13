package com.xgen.mongot.index.definition;

import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.version.Generation;
import com.xgen.mongot.util.Check;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.collections4.ListUtils;

public abstract class IndexDefinitionGenerationProducer {

  public abstract IndexDefinitionGeneration createIndexDefinitionGeneration(Generation generation);

  public abstract IndexDefinition getIndexDefinition();

  public SearchProducer asSearch() {
    Check.instanceOf(this, SearchProducer.class);
    return (SearchProducer) this;
  }

  public VectorProducer asVector() {
    Check.instanceOf(this, VectorProducer.class);
    return (VectorProducer) this;
  }

  public static List<IndexDefinitionGenerationProducer> createProducers(
      List<VectorIndexDefinition> vectorDefinitions,
      List<SearchIndexDefinition> searchDefinitions,
      List<OverriddenBaseAnalyzerDefinition> analyzerDefinitions) {
    var searchIndexes =
        searchDefinitions.stream()
            .map(
                definition ->
                    AnalyzerBoundSearchIndexDefinition.withRelevantOverriddenAnalyzers(
                        definition, analyzerDefinitions))
            .map(IndexDefinitionGenerationProducer.SearchProducer::new)
            .collect(Collectors.toList());

    var vectorIndexes =
        vectorDefinitions.stream()
            .map(IndexDefinitionGenerationProducer.VectorProducer::new)
            .collect(Collectors.toList());

    return ListUtils.union(searchIndexes, vectorIndexes);
  }

  public static class SearchProducer extends IndexDefinitionGenerationProducer {

    private final AnalyzerBoundSearchIndexDefinition analyzerBoundDefinition;

    public SearchProducer(AnalyzerBoundSearchIndexDefinition analyzerBoundDefinition) {
      this.analyzerBoundDefinition = analyzerBoundDefinition;
    }

    @Override
    public IndexDefinitionGeneration createIndexDefinitionGeneration(Generation generation) {
      return new SearchIndexDefinitionGeneration(this.analyzerBoundDefinition, generation);
    }

    @Override
    public SearchIndexDefinition getIndexDefinition() {
      return this.analyzerBoundDefinition.indexDefinition();
    }

    public AnalyzerBoundSearchIndexDefinition getAnalyzerBoundDefinition() {
      return this.analyzerBoundDefinition;
    }
  }

  public static class VectorProducer extends IndexDefinitionGenerationProducer {

    private final VectorIndexDefinition definition;

    public VectorProducer(VectorIndexDefinition definition) {
      this.definition = definition;
    }

    @Override
    public IndexDefinitionGeneration createIndexDefinitionGeneration(Generation generation) {
      return new VectorIndexDefinitionGeneration(this.definition, generation);
    }

    @Override
    public VectorIndexDefinition getIndexDefinition() {
      return this.definition;
    }
  }
}
