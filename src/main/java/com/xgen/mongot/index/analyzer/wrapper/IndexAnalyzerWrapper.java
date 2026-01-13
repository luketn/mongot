package com.xgen.mongot.index.analyzer.wrapper;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;

public class IndexAnalyzerWrapper extends DelegatingAnalyzerWrapper {

  private final Analyzer defaultAnalyzer;

  private final ImmutableMap<String, Analyzer> staticFieldAnalyzers;

  private final Optional<DynamicTypeSetPrefixMap<Analyzer>> dynamicFieldAnalyzersOptional;

  public IndexAnalyzerWrapper(
      Analyzer defaultAnalyzer,
      ImmutableMap<String, Analyzer> staticMapping,
      Optional<DynamicTypeSetPrefixMap<Analyzer>> maybeDynamicFieldAnalyzers) {
    super(PER_FIELD_REUSE_STRATEGY);
    this.defaultAnalyzer = defaultAnalyzer;
    this.staticFieldAnalyzers = staticMapping;
    this.dynamicFieldAnalyzersOptional = maybeDynamicFieldAnalyzers;
  }

  @Override
  protected Analyzer getWrappedAnalyzer(String luceneFieldName) {
    Optional<Analyzer> analyzerOptional =
        Optional.ofNullable(this.staticFieldAnalyzers.get(luceneFieldName))
            .or(
                () ->
                    this.dynamicFieldAnalyzersOptional.flatMap(
                        prefixMap -> prefixMap.getNearestRoot(luceneFieldName)));
    return analyzerOptional.orElse(this.defaultAnalyzer);
  }

  @Override
  public String toString() {
    return String.format(
        "IndexAnalyzerWrapper(staticFieldAnalyzers=%s, dynamicFieldAnalyzers=%s, default=%s)",
        this.staticFieldAnalyzers, this.dynamicFieldAnalyzersOptional, this.defaultAnalyzer);
  }
}
