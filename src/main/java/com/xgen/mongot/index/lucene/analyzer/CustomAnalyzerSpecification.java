package com.xgen.mongot.index.lucene.analyzer;

import com.xgen.mongot.index.analyzer.custom.CharFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenFilterDefinition;
import com.xgen.mongot.index.analyzer.custom.TokenizerDefinition;
import java.util.List;
import java.util.Optional;

public interface CustomAnalyzerSpecification {
  Optional<List<CharFilterDefinition>> charFilters();

  TokenizerDefinition tokenizer();

  Optional<List<TokenFilterDefinition>> tokenFilters();
}
