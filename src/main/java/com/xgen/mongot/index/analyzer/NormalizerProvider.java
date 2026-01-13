package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.definition.NormalizerDefinition;
import com.xgen.mongot.index.lucene.analyzer.CustomAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.core.KeywordTokenizer;

public class NormalizerProvider implements AnalyzerProvider.Normalizer {
  @Override
  public Analyzer getAnalyzer(NormalizerDefinition normalizerDefinition)
      throws InvalidAnalyzerDefinitionException {
    return switch (normalizerDefinition.normalizer()) {
      case NONE -> new KeywordAnalyzer();
      case LOWERCASE ->
          CustomAnalyzer.builder(KeywordTokenizer::new).tokenFilter(LowerCaseFilter::new).build();
    };
  }
}
