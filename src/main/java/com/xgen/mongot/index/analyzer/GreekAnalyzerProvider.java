package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.el.GreekAnalyzer;

public class GreekAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {

  /** Get Analyzer. */
  @Override
  public GreekAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException("GreekAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "GreekAnalyzer does not support stemExclusionSet");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      return new GreekAnalyzer(analyzerDefinition.getStopwords().get());
    }

    return new GreekAnalyzer();
  }
}
