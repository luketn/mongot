package com.xgen.testing.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.analyzer.InvalidAnalyzerDefinitionException;
import java.util.Collections;

/** Build analyzer registries for tests. */
public class AnalyzerRegistryBuilder {
  /** Create an empty registry. */
  public static AnalyzerRegistry empty() {
    try {
      return AnalyzerRegistry.factory().create(Collections.emptyList(), true);
    } catch (InvalidAnalyzerDefinitionException e) {
      throw new AssertionError(e);
    }
  }
}
