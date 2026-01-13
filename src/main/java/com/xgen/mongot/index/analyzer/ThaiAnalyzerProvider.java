package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.th.ThaiAnalyzer;

public class ThaiAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {

  /** Get Analyzer. */
  @Override
  public ThaiAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException("ThaiAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "ThaiAnalyzer does not support stemExclusionSet");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      return new ThaiAnalyzer(analyzerDefinition.getStopwords().get());
    }

    return new ThaiAnalyzer();
  }
}
