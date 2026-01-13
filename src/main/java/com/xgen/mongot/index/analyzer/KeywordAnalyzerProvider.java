package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.core.KeywordAnalyzer;

public class KeywordAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {

  /** Get Analyzer. * */
  @Override
  public KeywordAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "KeywordAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      throw new InvalidAnalyzerDefinitionException("KeywordAnalyzer does not support stopwords");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "KeywordAnalyzer does not support stemExclusionSet");
    }

    return new KeywordAnalyzer();
  }
}
