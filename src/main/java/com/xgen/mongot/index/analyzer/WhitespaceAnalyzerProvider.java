package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;

public class WhitespaceAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {

  /** Get Analyzer. * */
  @Override
  public WhitespaceAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "WhitespaceAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      throw new InvalidAnalyzerDefinitionException("WhitespaceAnalyzer does not support stopwords");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "WhitespaceAnalyzer does not support stemExclusionSet");
    }

    return new WhitespaceAnalyzer();
  }
}
