package com.xgen.mongot.index.analyzer.wrapper;

import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;

class IndexAnalyzerBuilder extends AnalyzerWrapperBuilder<Analyzer> {

  @Override
  Optional<Analyzer> getFieldNormalizer(
      AnalyzerRegistry analyzerRegistry, TokenFieldDefinition field) {
    return field.normalizer().map(analyzerRegistry::getNormalizer);
  }

  @Override
  Optional<Analyzer> getFieldAnalyzer(AnalyzerRegistry registry, StringFieldDefinition field) {
    return field.analyzerName().map(registry::getAnalyzer);
  }

  @Override
  Analyzer getAutoCompleteAnalyzer(AnalyzerRegistry registry, AutocompleteFieldDefinition field) {
    return registry.getAutocompleteAnalyzer(field);
  }
}
