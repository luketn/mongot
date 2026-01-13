package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamType;
import com.xgen.mongot.index.analyzer.attributes.TokenStreamTypeAware;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.util.Check;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TokenStreamTypeProvider is used to fetch a {@link TokenStreamType} for various analyzer types.
 */
public record TokenStreamTypeProvider(
    Map<String, OverriddenBaseAnalyzerDefinition> overriddenAnalyzers,
    Map<String, CustomAnalyzerDefinition> customAnalyzers) {
  private static final Logger logger = LoggerFactory.getLogger(TokenStreamTypeProvider.class);

  /**
   * Finds the analyzer definition by name, and returns a value indicating if it's a graph-output
   * analyzer or a stream-output analyzer.
   *
   * @return {@literal true} if the analyzer is a graph-output analyzer, {@literal false} if it's a
   *     stream-output analyzer.
   * @throws AssertionError if the analyzer was not found.
   */
  public boolean isGraphOrThrow(String analyzerName) {
    return get(analyzerName)
        .orElseThrow(() -> new AssertionError("Invalid analyzer name: %s".formatted(analyzerName)))
        .isGraph();
  }

  private Optional<TokenStreamType> get(String analyzerName) {
    OverriddenBaseAnalyzerDefinition overriddenAnalyzer;
    if ((overriddenAnalyzer = this.overriddenAnalyzers.get(analyzerName)) != null) {
      return getTokenStreamType(overriddenAnalyzer);
    }

    CustomAnalyzerDefinition customAnalyzer;
    if ((customAnalyzer = this.customAnalyzers.get(analyzerName)) != null) {
      return getTokenStreamType(customAnalyzer);
    }

    return stockAnalyzerLookup(analyzerName);
  }

  /** TokenStreamType for this analyzer. */
  static Optional<TokenStreamType> getTokenStreamType(AnalyzerDefinition definition) {
    return switch (definition) {
      case OverriddenBaseAnalyzerDefinition overriddenBaseAnalyzerDefinition ->
          stockAnalyzerLookup(overriddenBaseAnalyzerDefinition.getBaseAnalyzerName());
      case CustomAnalyzerDefinition customAnalyzerDefinition ->
          Optional.of(customAnalyzerDefinition.getTokenStreamType());
      default -> {
        logger.error("Unexpected AnalyzerDefinition type: {}", definition.getClass().getName());
        yield Check.unreachable("Unexpected AnalyzerDefinition type");
      }
    };
  }

  private static Optional<TokenStreamType> stockAnalyzerLookup(String analyzerName) {
    return LuceneAnalyzerProviders.baseAnalyzerProviderFor(analyzerName)
        .map(TokenStreamTypeAware::getTokenStreamType);
  }
}
