package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;

/** Analyzer for Japanese. */
public class KuromojiAnalyzerProvider
    implements AnalyzerProvider.OverriddenBase, TokenStreamTypeAware.Graph {
  @Override
  public JapaneseAnalyzer getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    if (analyzerDefinition.getMaxTokenLength().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "KurimojiAnalyzer does not support maxTokenLength");
    }

    if (analyzerDefinition.getStemExclusionSet().isPresent()) {
      throw new InvalidAnalyzerDefinitionException(
          "KurmojiAnalyzer does not support stemExclusionSet");
    }

    if (analyzerDefinition.getStopwords().isPresent()) {
      throw new InvalidAnalyzerDefinitionException("KurmojiAnalyzer does not support stopwords");
    }

    return new JapaneseAnalyzer();
  }
}
