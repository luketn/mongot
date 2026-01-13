package com.xgen.mongot.index.analyzer.wrapper;

import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_ANALYZER;
import static com.xgen.mongot.index.definition.SearchIndexDefinition.DEFAULT_FALLBACK_NORMALIZER;

import com.google.common.collect.ImmutableMap;
import com.xgen.mongot.index.analyzer.AnalyzerMeta;
import com.xgen.mongot.index.analyzer.AnalyzerRegistry;
import com.xgen.mongot.index.definition.SearchIndexDefinition;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;

public class LuceneAnalyzer {

  /**
   * Create our PerFieldAnalyzerWrapper, which is a wrapper for the Analyzer class. Currently, all
   * dynamically discovered fields will use the default configured Analyzer or StandardAnalyzer if
   * none is configured. As such, we won't have update the PerFieldAnalyzerWrapper when a field is
   * added.
   *
   * <p>Any fields that don't exist in it will simply use the proper default analyzer. This may
   * change if we add dynamic templates, allowing for different dynamically mapped fields to have
   * non-default Analyzers. If that happens this design may need to be reconsidered.
   *
   * <p>For each static field mapping, the returned analyzer will select one of the following
   * analyzers in descending order of preference:
   *
   * <ol>
   *   <li>index.mappings.field.analyzer
   *   <li>index.analyzer
   *   <li>{@link SearchIndexDefinition#DEFAULT_FALLBACK_ANALYZER}
   * </ol>
   */
  public static Analyzer indexAnalyzer(
      SearchIndexDefinition indexDefinition, AnalyzerRegistry analyzerRegistry) {
    Analyzer defaultAnalyzer =
        analyzerRegistry.getAnalyzer(
            indexDefinition.getAnalyzerName().orElse(DEFAULT_FALLBACK_ANALYZER));
    Analyzer defaultNormalizer = analyzerRegistry.getNormalizer(DEFAULT_FALLBACK_NORMALIZER);
    ImmutableMap<String, Analyzer> staticMapping =
        new IndexAnalyzerBuilder()
            .buildStaticMappings(
                indexDefinition, analyzerRegistry, defaultAnalyzer, defaultNormalizer);
    Optional<DynamicTypeSetPrefixMap<Analyzer>> maybeDynamicTypeSetPrefixMap =
        new IndexAnalyzerBuilder()
            .buildDynamicMappings(
                indexDefinition, analyzerRegistry, defaultAnalyzer, defaultNormalizer);
    return new IndexAnalyzerWrapper(defaultAnalyzer, staticMapping, maybeDynamicTypeSetPrefixMap);
  }

  /**
   * Creates an Analyzer for query strings that delegates on a per-field basis to the appropriate
   * analyzer according to the {@link SearchIndexDefinition}.
   *
   * <p>For each static field mapping, the returned analyzer will select one of the following
   * analyzers in descending order of preference:
   *
   * <ol>
   *   <li>index.mappings.field.searchAnalyzer
   *   <li>index.mappings.field.analyzer
   *   <li>index.searchAnalyzer
   *   <li>index.analyzer
   *   <li>{@link SearchIndexDefinition#DEFAULT_FALLBACK_ANALYZER}
   * </ol>
   */
  public static QueryAnalyzerWrapper queryAnalyzer(
      SearchIndexDefinition index, AnalyzerRegistry analyzerRegistry) {
    String defaultAnalyzerName =
        index.getSearchAnalyzerName().or(index::getAnalyzerName).orElse(DEFAULT_FALLBACK_ANALYZER);
    AnalyzerMeta defaultAnalyzer = analyzerRegistry.getAnalyzerMeta(defaultAnalyzerName);
    AnalyzerMeta defaultNormalizer =
        analyzerRegistry.getNormalizerMeta(DEFAULT_FALLBACK_NORMALIZER);
    ImmutableMap<String, AnalyzerMeta> staticMapping =
        new QueryAnalyzerBuilder()
            .buildStaticMappings(index, analyzerRegistry, defaultAnalyzer, defaultNormalizer);
    Optional<DynamicTypeSetPrefixMap<AnalyzerMeta>> maybeDynamicTypeSetPrefixMap =
        new QueryAnalyzerBuilder()
            .buildDynamicMappings(index, analyzerRegistry, defaultAnalyzer, defaultNormalizer);
    return new QueryAnalyzerWrapper(
        index, defaultAnalyzer, staticMapping, maybeDynamicTypeSetPrefixMap);
  }
}
