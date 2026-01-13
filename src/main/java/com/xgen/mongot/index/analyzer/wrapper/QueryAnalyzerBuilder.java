package com.xgen.mongot.index.analyzer.wrapper;

import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.index.definition.StringFieldDefinition;
import com.xgen.mongot.index.definition.TokenFieldDefinition;
import java.util.Optional;

class QueryAnalyzerBuilder extends AnalyzerWrapperBuilder<AnalyzerMeta> {

  @Override
  Optional<AnalyzerMeta> getFieldNormalizer(
      AnalyzerRegistry analyzerRegistry, TokenFieldDefinition field) {
    return field.normalizer().map(analyzerRegistry::getNormalizerMeta);
  }

  @Override
  Optional<AnalyzerMeta> getFieldAnalyzer(AnalyzerRegistry registry, StringFieldDefinition field) {
    return field.searchAnalyzerName().or(field::analyzerName).map(registry::getAnalyzerMeta);
  }

  @Override
  AnalyzerMeta getAutoCompleteAnalyzer(
      AnalyzerRegistry registry, AutocompleteFieldDefinition field) {
    return registry.getAnalyzerMeta(field.getAnalyzer());
  }
}
