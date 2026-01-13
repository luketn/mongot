package com.xgen.mongot.index.analyzer;

import static com.xgen.testing.mongot.index.analyzer.AnalyzerTestUtil.createAnalyzerContainerOrFail;

import com.xgen.mongot.index.analyzer.definition.AnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import com.xgen.mongot.index.analyzer.definition.StockAnalyzerNames;
import com.xgen.mongot.util.functionalinterfaces.CheckedSupplier;
import com.xgen.testing.TestUtils;
import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.analyzer.definition.CustomAnalyzerDefinitionBuilder;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.junit.Assert;
import org.junit.Test;

public class AutocompleteAnalyzerSpecificationTest {

  @Test
  public void testEquals() throws Exception {
    TestUtils.assertEqualityGroups(
        Builder.withMinGram(3),
        Builder.withMinGram(4),
        Builder.withMaxGram(4),
        Builder.withMaxGram(5),
        Builder.withFoldDiacritics(false),
        Builder.withFoldDiacritics(true),
        Builder.withTokenizationStrategy(
            AutocompleteAnalyzerSpecification.TokenizationStrategy.NGRAM),
        Builder.withAnalyzer("lucene.spanish"),
        Builder.withAnalyzer(
            new CustomAnalyzerDefinitionBuilder(
                    "myAnalyzer", TokenizerDefinitionBuilder.WhitespaceTokenizer.builder().build())
                .build()));
  }

  @Test
  public void testAnalyzerDoesNotEffectEquality() {
    AutocompleteAnalyzerSpecification first =
        new AutocompleteAnalyzerSpecification(
            1,
            5,
            true,
            AutocompleteAnalyzerSpecification.TokenizationStrategy.EDGE_GRAM,
            new AnalyzerContainer(
                OverriddenBaseAnalyzerDefinition.stockAnalyzerWithName("lucene.standard"),
                new StandardAnalyzer()));

    AutocompleteAnalyzerSpecification second =
        new AutocompleteAnalyzerSpecification(
            1,
            5,
            true,
            AutocompleteAnalyzerSpecification.TokenizationStrategy.EDGE_GRAM,
            new AnalyzerContainer(
                OverriddenBaseAnalyzerDefinition.stockAnalyzerWithName("lucene.standard"),
                new KeywordAnalyzer()));

    Assert.assertEquals(
        "analyzer containers with same analyzer definition should be equal", first, second);
  }

  static class Builder {
    private static final int DEFAULT_MIN_GRAM = 1;
    private static final int DEFAULT_MAX_GRAM = 6;
    private static final boolean DEFAULT_FOLD_DIACRITICS = true;
    private static final AutocompleteAnalyzerSpecification.TokenizationStrategy
        DEFAULT_TOKENIZATION_STRATEGY =
            AutocompleteAnalyzerSpecification.TokenizationStrategy.EDGE_GRAM;
    private static final AnalyzerContainer DEFAULT_ANALYZER =
        createAnalyzerContainerOrFail(StockAnalyzerNames.LUCENE_STANDARD);

    static CheckedSupplier<AutocompleteAnalyzerSpecification, Exception> withMinGram(int minGram) {
      return () ->
          new AutocompleteAnalyzerSpecification(
              minGram,
              DEFAULT_MAX_GRAM,
              DEFAULT_FOLD_DIACRITICS,
              DEFAULT_TOKENIZATION_STRATEGY,
              DEFAULT_ANALYZER);
    }

    static CheckedSupplier<AutocompleteAnalyzerSpecification, Exception> withMaxGram(int maxGram) {
      return () ->
          new AutocompleteAnalyzerSpecification(
              DEFAULT_MIN_GRAM,
              maxGram,
              DEFAULT_FOLD_DIACRITICS,
              DEFAULT_TOKENIZATION_STRATEGY,
              DEFAULT_ANALYZER);
    }

    static CheckedSupplier<AutocompleteAnalyzerSpecification, Exception> withFoldDiacritics(
        boolean foldDiacritics) {
      return () ->
          new AutocompleteAnalyzerSpecification(
              DEFAULT_MIN_GRAM,
              DEFAULT_MAX_GRAM,
              foldDiacritics,
              DEFAULT_TOKENIZATION_STRATEGY,
              DEFAULT_ANALYZER);
    }

    static CheckedSupplier<AutocompleteAnalyzerSpecification, Exception> withTokenizationStrategy(
        AutocompleteAnalyzerSpecification.TokenizationStrategy tokenizationStrategy) {
      return () ->
          new AutocompleteAnalyzerSpecification(
              DEFAULT_MIN_GRAM,
              DEFAULT_MAX_GRAM,
              DEFAULT_FOLD_DIACRITICS,
              tokenizationStrategy,
              DEFAULT_ANALYZER);
    }

    static CheckedSupplier<AutocompleteAnalyzerSpecification, Exception> withAnalyzer(
        AnalyzerDefinition definition) {
      return () ->
          new AutocompleteAnalyzerSpecification(
              DEFAULT_MIN_GRAM,
              DEFAULT_MAX_GRAM,
              DEFAULT_FOLD_DIACRITICS,
              DEFAULT_TOKENIZATION_STRATEGY,
              createAnalyzerContainerOrFail(definition));
    }

    static CheckedSupplier<AutocompleteAnalyzerSpecification, Exception> withAnalyzer(
        String analyzerName) {
      return () ->
          new AutocompleteAnalyzerSpecification(
              DEFAULT_MIN_GRAM,
              DEFAULT_MAX_GRAM,
              DEFAULT_FOLD_DIACRITICS,
              DEFAULT_TOKENIZATION_STRATEGY,
              createAnalyzerContainerOrFail(analyzerName));
    }
  }
}
