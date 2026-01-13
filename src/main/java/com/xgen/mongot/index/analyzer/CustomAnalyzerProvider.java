package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.definition.CustomAnalyzerDefinition;
import com.xgen.mongot.index.lucene.analyzer.LuceneAnalyzerFactory;
import java.time.Duration;
import org.apache.lucene.analysis.Analyzer;

public class CustomAnalyzerProvider implements AnalyzerProvider.Custom {
  private static final CustomAnalyzerProvider PROVIDER_INSTANCE =
      new CustomAnalyzerProvider(
          CachingAnalyzerProviderFactory.expiringAfterNoAccessPeriod(
              LuceneAnalyzerFactory::build, Duration.ofDays(1)));

  private final AnalyzerFactory<CustomAnalyzerDefinition> factory;

  private CustomAnalyzerProvider(AnalyzerFactory<CustomAnalyzerDefinition> factory) {
    this.factory = factory;
  }

  static Analyzer provide(CustomAnalyzerDefinition definition) {
    return PROVIDER_INSTANCE.getAnalyzer(definition);
  }

  @Override
  public Analyzer getAnalyzer(CustomAnalyzerDefinition definition) {
    return this.factory.getAnalyzer(definition);
  }
}
