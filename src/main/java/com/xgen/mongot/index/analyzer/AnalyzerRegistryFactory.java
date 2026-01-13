package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import java.util.Collection;

public interface AnalyzerRegistryFactory {
  AnalyzerRegistry create(
      Collection<? extends AnalyzerDefinition> definitions,
      boolean enableAutocompleteTruncateTokens)
      throws InvalidAnalyzerDefinitionException;
}
