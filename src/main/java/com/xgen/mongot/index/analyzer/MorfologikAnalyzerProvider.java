package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.morfologik.MorfologikAnalyzer;

/** Analyzer for Polish. */
public class MorfologikAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {
  @Override
  public MorfologikAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "MorfologikAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      throw new InvalidAnalyzerDefinitionException("MorfologikAnalyzer does not support stopwords");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "MorfologikAnalyzer does not support stemExclusionSet");
    }

    return new MorfologikAnalyzer();
  }
}
