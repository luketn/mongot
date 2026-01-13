package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.NormalizerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import org.apache.lucene.analysis.Analyzer;

public interface AnalyzerProvider<D extends AnalyzerDefinition> {
  Analyzer getAnalyzer(D analyzerDefinition) throws InvalidAnalyzerDefinitionException;

  interface OverriddenBase
      extends AnalyzerProvider<OverriddenBaseAnalyzerDefinition>, TokenStreamTypeAware {}

  interface Custom extends AnalyzerProvider<CustomAnalyzerDefinition> {}

  interface Normalizer
      extends AnalyzerProvider<NormalizerDefinition>, TokenStreamTypeAware.Stream {}
}
