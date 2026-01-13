package com.xgen.mongot.index.analyzer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.NormalizerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.util.CheckedStream;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.analysis.Analyzer;

public record AnalyzerContainer(AnalyzerDefinition definition, Analyzer analyzer) {
  private static class Builder {
    private static <T extends AnalyzerDefinition> Analyzer createAnalyzer(T definition)
        throws InvalidAnalyzerDefinitionException {
      return switch (definition) {
        case OverriddenBaseAnalyzerDefinition overriddenBaseAnalyzerDefinition ->
            new TokenByteSizeFilterAnalyzer(
                createOverriddenAnalyzer(overriddenBaseAnalyzerDefinition));
        case CustomAnalyzerDefinition customAnalyzerDefinition ->
            new TokenByteSizeFilterAnalyzer(createCustomAnalyzer(customAnalyzerDefinition));
        case NormalizerDefinition normalizerDefinition ->
            new TokenByteSizeFilterAnalyzer(createNormalizer(normalizerDefinition));
      };
    }

    private static Analyzer createOverriddenAnalyzer(
        OverriddenBaseAnalyzerDefinition analyzerDefinition)
        throws InvalidAnalyzerDefinitionException {
      return LuceneAnalyzerProviders.baseAnalyzerProviderFor(
              analyzerDefinition.getBaseAnalyzerName())
          .orElseThrow(
              () ->
                  InvalidAnalyzerDefinitionException.analyzerNotFound(
                      analyzerDefinition.getBaseAnalyzerName()))
          .getAnalyzer(analyzerDefinition);
    }

    private static Analyzer createCustomAnalyzer(CustomAnalyzerDefinition analyzerDefinition) {
      return CustomAnalyzerProvider.provide(analyzerDefinition);
    }

    private static Analyzer createNormalizer(NormalizerDefinition normalizerDefinition)
        throws InvalidAnalyzerDefinitionException {
      return new NormalizerProvider().getAnalyzer(normalizerDefinition);
    }
  }

  public static AnalyzerContainer create(AnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    // make sure new analyzer is not clobbering a stock analyzer name
    if (LuceneAnalyzerProviders.hasStockAnalyzerNamed(analyzerDefinition.name())) {
      throw InvalidAnalyzerDefinitionException.nameClashesWithStockAnalyzer(
          analyzerDefinition.name());
    }

    return createUnchecked(analyzerDefinition);
  }

  static List<AnalyzerContainer> createAllStockAnalyzers()
      throws InvalidAnalyzerDefinitionException {
    return CheckedStream.from(LuceneAnalyzerProviders.allStockAnalyzers())
        .mapAndCollectChecked(AnalyzerContainer::createUnchecked);
  }

  static ImmutableMap<StockNormalizerName, AnalyzerContainer> createAllStockNormalizers()
      throws InvalidAnalyzerDefinitionException {

    Map<StockNormalizerName, AnalyzerContainer> builder = new EnumMap<>(StockNormalizerName.class);
    for (StockNormalizerName name : StockNormalizerName.values()) {
      NormalizerDefinition definition = NormalizerDefinition.stockNormalizer(name);
      builder.put(name, AnalyzerContainer.createUnchecked(definition));
    }

    return Maps.immutableEnumMap(builder);
  }

  private static AnalyzerContainer createUnchecked(AnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    return new AnalyzerContainer(analyzerDefinition, Builder.createAnalyzer(analyzerDefinition));
  }

  /**
   * Checks if this {@link AnalyzerContainer} equals another. Only checks if the {@link
   * AnalyzerDefinition} of the container is equivalent.
   *
   * <p>This assumes that the {@link Analyzer} of this {@link AnalyzerContainer} was created from
   * the {@link AnalyzerDefinition} that is also a member of this class, and relies on the fact that
   * equivalent {@link AnalyzerDefinition}s produce equivalent {@link Analyzer}s.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AnalyzerContainer that = (AnalyzerContainer) o;
    return this.definition.equals(that.definition);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.definition);
  }
}
