package com.xgen.mongot.index.analyzer;

import static com.xgen.mongot.util.Check.checkState;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockNormalizerName;
import com.xgen.mongot.index.definition.AutocompleteFieldDefinition;
import com.xgen.mongot.util.CheckedStream;
import com.xgen.mongot.util.CollectionUtils;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.lucene.analysis.Analyzer;

/**
 * This analyzer registry is immutable and thread safe. Creating it via the factory allows for
 * multiple instances to share the same stock analyzers.
 */
public class AnalyzerRegistry {

  private final Map<String, AnalyzerContainer> stockAnalyzers;
  private final ImmutableMap<StockNormalizerName, AnalyzerContainer> stockNormalizers;
  private final Map<String, AnalyzerContainer> analyzers;
  private final AutocompleteAnalyzerProvider autocompleteAnalyzerProvider;

  private AnalyzerRegistry(
      Map<String, AnalyzerContainer> stockAnalyzers,
      ImmutableMap<StockNormalizerName, AnalyzerContainer> stockNormalizers,
      List<AnalyzerContainer> analyzers,
      boolean enableAutocompleteTruncateTokens) {
    this.stockAnalyzers = stockAnalyzers;
    this.stockNormalizers = stockNormalizers;
    this.analyzers = byName(analyzers);
    this.autocompleteAnalyzerProvider =
        new AutocompleteAnalyzerProvider(enableAutocompleteTruncateTokens);
  }

  /**
   * There should only be one factory present in mongot as it shares references to all stock
   * analyzers, If this method fails with {@code InvalidAnalyzerDefinitionException}, it can only
   * mean that some stock analyzers are invalid.
   */
  public static Factory factory() throws InvalidAnalyzerDefinitionException {
    return new Factory();
  }

  public static class Factory implements AnalyzerRegistryFactory {
    private final Map<String, AnalyzerContainer> stockAnalyzers;
    private final ImmutableMap<StockNormalizerName, AnalyzerContainer> stockNormalizers;

    private Factory() throws InvalidAnalyzerDefinitionException {
      this.stockAnalyzers = byName(AnalyzerContainer.createAllStockAnalyzers());
      this.stockNormalizers = AnalyzerContainer.createAllStockNormalizers();
    }

    /**
     * Create a registry for custom and overridden analyzer definitions.
     *
     * @throws InvalidAnalyzerDefinitionException If the definitions are clobbering stock analyzers,
     *     contain duplicates, or invalid.
     */
    @Override
    public AnalyzerRegistry create(
        Collection<? extends AnalyzerDefinition> definitions,
        boolean enableAutocompleteTruncateTokens)
        throws InvalidAnalyzerDefinitionException {
      validateUniqueNames(definitions);

      return new AnalyzerRegistry(
          this.stockAnalyzers,
          this.stockNormalizers,
          CheckedStream.from(definitions).mapAndCollectChecked(AnalyzerContainer::create),
          enableAutocompleteTruncateTokens);
    }

    private static void validateUniqueNames(Collection<? extends AnalyzerDefinition> definitions)
        throws InvalidAnalyzerDefinitionException {
      Set<String> duplicates =
          CollectionUtils.findDuplicates(
              definitions.stream().map(AnalyzerDefinition::name).collect(Collectors.toList()));
      if (!duplicates.isEmpty()) {
        throw new InvalidAnalyzerDefinitionException(
            "More than one analyzer with the same name present: " + String.join(", ", duplicates));
      }
    }
  }

  /** Checks to ensure the AnalyzerDefinition of an overridden AnalyzerDefinition is valid. */
  public static void validateAnalyzerDefinition(AnalyzerDefinition analyzerDefinition)
      throws InvalidAnalyzerDefinitionException {
    // TODO(CLOUDP-280897): refactor AnalyzerProvider to have a validate method
    AnalyzerContainer.create(analyzerDefinition);
  }

  static boolean isStockAnalyzerName(String name) {
    return LuceneAnalyzerProviders.hasStockAnalyzerNamed(name);
  }

  /**
   * Returns the Analyzer registered with the given name. Throws an IllegalStateException if the
   * Analyzer does not exist.
   */
  public Analyzer getAnalyzer(String analyzerName) {
    return getAnalyzerContainer(analyzerName).analyzer();
  }

  /** Returns the Normalizer registered with the given name. */
  public Analyzer getNormalizer(StockNormalizerName normalizer) {
    return getNormalizerContainer(normalizer).analyzer();
  }

  /**
   * Get an analyzer with some metadata. Throws an IllegalStateException if the Analyzer does not
   * exist.
   */
  public AnalyzerMeta getAnalyzerMeta(String analyzerName) {
    AnalyzerContainer container = getAnalyzerContainer(analyzerName);
    return new AnalyzerMeta(container.analyzer(), container.definition());
  }

  public AnalyzerMeta getNormalizerMeta(StockNormalizerName normalizer) {
    AnalyzerContainer container = getNormalizerContainer(normalizer);
    return new AnalyzerMeta(container.analyzer(), container.definition());
  }

  private AnalyzerContainer getNormalizerContainer(StockNormalizerName normalizer) {
    return this.stockNormalizers.get(normalizer);
  }

  private AnalyzerContainer getAnalyzerContainer(String analyzerName) {
    if (this.stockAnalyzers.containsKey(analyzerName)) {
      return this.stockAnalyzers.get(analyzerName);
    }

    checkState(
        this.analyzers.containsKey(analyzerName), "analyzer %s does not exist", analyzerName);

    return this.analyzers.get(analyzerName);
  }

  public Analyzer getAutocompleteAnalyzer(AutocompleteFieldDefinition fieldDefinition) {
    return this.autocompleteAnalyzerProvider.getAnalyzer(
        AutocompleteAnalyzerSpecification.create(
            fieldDefinition, getAnalyzerContainer(fieldDefinition.getAnalyzer())));
  }

  /** Definitions of overridden analyzers. */
  public List<AnalyzerDefinition> getAnalyzerDefinitions() {
    return this.analyzers.values().stream()
        .map(AnalyzerContainer::definition)
        .collect(Collectors.toList());
  }

  private static Map<String, AnalyzerContainer> byName(
      Collection<AnalyzerContainer> analyzerContainers) {
    return analyzerContainers.stream()
        .collect(
            CollectionUtils.toUnmodifiableMapUnsafe(
                container -> container.definition().name(), Function.identity()));
  }
}
