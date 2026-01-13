package com.xgen.mongot.index.analyzer;

import static com.xgen.testing.mongot.index.analyzer.AnalyzerTestUtil.createAnalyzerContainerOrFail;

import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.util.CheckedStream;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.analysis.Analyzer;
import org.junit.Assert;
import org.junit.Test;

public class CachingAnalyzerProviderFactoryTest {
  private static final AutocompleteAnalyzerSpecification ANALYZER_DEFINITION_ONE =
      new AutocompleteAnalyzerSpecification(
          1,
          2,
          true,
          AutocompleteAnalyzerSpecification.TokenizationStrategy.EDGE_GRAM,
          createAnalyzerContainerOrFail(StockAnalyzerNames.LUCENE_STANDARD));

  private static final AutocompleteAnalyzerSpecification ANALYZER_DEFINITION_TWO =
      new AutocompleteAnalyzerSpecification(
          1,
          2,
          false,
          AutocompleteAnalyzerSpecification.TokenizationStrategy.EDGE_GRAM,
          createAnalyzerContainerOrFail(StockAnalyzerNames.LUCENE_STANDARD));

  private static final int MAX_TOKENS = 20_000_000;

  @Test
  public void testCacheReturnsSameObject() {
    var provider =
        CachingAnalyzerProviderFactory.expiringAfterNoAccessPeriod(
            spec ->
                new AutocompleteAnalyzerProvider.AutocompleteAnalyzer(
                    (AutocompleteAnalyzerSpecification) spec, true, MAX_TOKENS),
            Duration.ofSeconds(10));

    Analyzer analyzerOne = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);
    Analyzer analyzerTwo = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);

    Assert.assertSame("returned analyzers should be the same object", analyzerOne, analyzerTwo);
  }

  @Test
  public void testCacheReturnsDifferentObject() {
    var provider =
        CachingAnalyzerProviderFactory.expiringAfterNoAccessPeriod(
            spec ->
                new AutocompleteAnalyzerProvider.AutocompleteAnalyzer(
                    (AutocompleteAnalyzerSpecification) spec, true, MAX_TOKENS),
            Duration.ofSeconds(10));

    Analyzer analyzerOne = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);
    Analyzer analyzerTwo = provider.getAnalyzer(ANALYZER_DEFINITION_TWO);

    Assert.assertNotSame(
        "returned analyzers should not be the same object", analyzerOne, analyzerTwo);
  }

  @Test
  public void testCacheExpiration() throws Exception {
    var provider =
        CachingAnalyzerProviderFactory.expiringAfterNoAccessPeriod(
            spec ->
                new AutocompleteAnalyzerProvider.AutocompleteAnalyzer(
                    (AutocompleteAnalyzerSpecification) spec, true, MAX_TOKENS),
            Duration.ofSeconds(1));

    Analyzer analyzerOne = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);
    Thread.sleep(1500);
    Analyzer analyzerTwo = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);

    Assert.assertNotSame(
        "analyzers created more than one second from last access should not be the same object",
        analyzerOne,
        analyzerTwo);
  }

  @Test
  public void testMultiThreadCacheSharing() throws Exception {
    var provider =
        CachingAnalyzerProviderFactory.expiringAfterNoAccessPeriod(
            spec ->
                new AutocompleteAnalyzerProvider.AutocompleteAnalyzer(
                    (AutocompleteAnalyzerSpecification) spec, true, MAX_TOKENS),
            Duration.ofSeconds(1));

    ExecutorService executorService = Executors.newFixedThreadPool(3);

    // Add sleep calls in each of these to prevent the case where all of these complete quickly
    // enough to be run in the same thread.
    List<Callable<Analyzer>> getters =
        List.of(
            () -> {
              Analyzer analyzer = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);
              Thread.sleep(500);
              return analyzer;
            },
            () -> {
              Analyzer analyzer = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);
              Thread.sleep(500);
              return analyzer;
            },
            () -> {
              Analyzer analyzer = provider.getAnalyzer(ANALYZER_DEFINITION_ONE);
              Thread.sleep(500);
              return analyzer;
            });

    List<Future<Analyzer>> analyzerFutures = executorService.invokeAll(getters);
    List<Analyzer> analyzers =
        CheckedStream.from(analyzerFutures).mapAndCollectChecked(Future::get);

    Assert.assertSame(
        "first and second analyzer should be the same", analyzers.get(0), analyzers.get(1));
    Assert.assertSame(
        "first and third analyzer should be the same", analyzers.get(0), analyzers.get(2));
  }
}
