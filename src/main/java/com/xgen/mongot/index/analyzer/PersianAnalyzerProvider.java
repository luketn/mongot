package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.fa.PersianAnalyzer;

public class PersianAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {

  /** Get Analyzer. */
  @Override
  public PersianAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "PersianAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "PersianAnalyzer does not support stemExclusionSet");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      return new PersianAnalyzer(analyzerDefinition.getStopwords().get());
    }

    return new PersianAnalyzer();
  }
}
