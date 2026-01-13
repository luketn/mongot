package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.uk.UkrainianMorfologikAnalyzer;

public class UkrainianMorfologikAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {
  @Override
  public UkrainianMorfologikAnalyzer getAnalyzer(
      OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "UkrainianMorfologikAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "UkrainianMorfologikAnalyzer does not support stemExclusionSet");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "UkrainianMorfologikAnalyzer does not support stopwords");
    }

    return new UkrainianMorfologikAnalyzer();
  }
}
