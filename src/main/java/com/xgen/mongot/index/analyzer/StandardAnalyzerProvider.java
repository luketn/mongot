package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class StandardAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Stream {

  /** Get Analyzer. * */
  @Override
  public StandardAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "StandardAnalyzer does not support stemExclusionSet");
    }

    StandardAnalyzer analyzer;

    if (analyzerDefinition.getStopwords().isPresent()) {
      analyzer = new StandardAnalyzer(analyzerDefinition.getStopwords().get());
    } else {
      analyzer = new StandardAnalyzer();
    }

    analyzerDefinition.getMaxTokenLength().ifPresent(analyzer::setMaxTokenLength);

    return analyzer;
  }
}
