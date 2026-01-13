package com.xgen.mongot.index.analyzer.definition;

/**
 * Normalizers are created from NormalizerDefinitions. The key differentiation between Normalizers
 * and Analyzers is that Normalizers produce a single token at the end of analysis where Analyzers
 * may produce 1 or more.
 */
public record NormalizerDefinition(StockNormalizerName normalizer) implements AnalyzerDefinition {

  public static NormalizerDefinition stockNormalizer(StockNormalizerName normalizer) {
    return new NormalizerDefinition(normalizer);
  }

  @Override
  public String name() {
    return this.normalizer.getNormalizerName();
  }
}
