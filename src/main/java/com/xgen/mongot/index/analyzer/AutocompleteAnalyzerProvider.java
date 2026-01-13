package com.xgen.mongot.index.analyzer;

import com.google.errorprone.annotations.Var;
import java.time.Duration;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;

class AutocompleteAnalyzerProvider {
  private static final int MAX_TOKENS = 20_000_000;
  private final AnalyzerFactory<AutocompleteAnalyzerSpecification> cachingInstance;

  AutocompleteAnalyzerProvider(boolean enableAutocompleteTruncateTokens) {
    this.cachingInstance =
        CachingAnalyzerProviderFactory.expiringAfterNoAccessPeriod(
            spec -> new AutocompleteAnalyzer(spec, enableAutocompleteTruncateTokens, MAX_TOKENS),
            Duration.ofDays(1));
  }

  /**
   * Get the analyzer for this {@link AutocompleteAnalyzerSpecification}, first checking the cache
   * to see if one has recently been created or used.
   */
  Analyzer getAnalyzer(AutocompleteAnalyzerSpecification analyzerSpecification) {
    return this.cachingInstance.getAnalyzer(analyzerSpecification);
  }

  static class AutocompleteAnalyzer extends AnalyzerWrapper {
    private final AutocompleteAnalyzerSpecification analyzerSpecification;
    private final boolean enableAutocompleteTruncateTokens;
    private final int maxTokens;

    AutocompleteAnalyzer(
        AutocompleteAnalyzerSpecification specification,
        boolean enableAutocompleteTruncateTokens,
        int maxTokens) {
      super(specification.getBaseAnalyzerContainer().analyzer().getReuseStrategy());
      this.analyzerSpecification = specification;
      this.enableAutocompleteTruncateTokens = enableAutocompleteTruncateTokens;
      this.maxTokens = maxTokens;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
      return this.analyzerSpecification.getBaseAnalyzerContainer().analyzer();
    }

    @Override
    protected TokenStream wrapTokenStreamForNormalization(String fieldName, TokenStream in) {
      @Var TokenStream tokenStream = in;

      // 1. Strip diacritics, if configured to do so.
      if (this.analyzerSpecification.isFoldDiacritics()) {
        tokenStream = new ICUFoldingFilter(tokenStream);
      }

      // 2. Reverse token characters if indexed tokenization strategy is rightEdgeGram
      if (this.analyzerSpecification.getTokenizationStrategy()
          == AutocompleteAnalyzerSpecification.TokenizationStrategy.RIGHT_EDGE_GRAM) {
        tokenStream = new ReverseStringFilter(tokenStream);
      }

      // 3. Truncate token terms to maxGrams length.
      tokenStream = new TruncateTokenFilter(tokenStream, this.analyzerSpecification.getMaxGrams());

      return tokenStream;
    }

    @Override
    protected TokenStreamComponents wrapComponents(
        String fieldName, TokenStreamComponents components) {
      return switch (this.analyzerSpecification.getTokenizationStrategy()) {
        case EDGE_GRAM -> wrapComponentsForEdgeNgram(components);
        case NGRAM -> wrapComponentsForNgram(components);
        case RIGHT_EDGE_GRAM -> wrapComponentsForRightEdgeNgram(components);
      };
    }

    private TokenStreamComponents wrapComponentsForEdgeNgram(TokenStreamComponents components) {
      CustomTokenStreamBuilder builder = CustomTokenStreamBuilder.builder(components);

      // 1. Strip diacritics, if configured to do so.
      if (this.analyzerSpecification.isFoldDiacritics()) {
        builder.appendDiacriticStripping();
      }

      /*
       * 2. Group words, as tokenized by the user-specified analyzer, into groups of two or three.
       *    We do this so that edgeGrams can span word boundaries.
       * 3. Apply the edgeGramTokenFilter, splitting sequences into many edgeGrams.
       * 4. Ensure edgeGrams passed through the edgeGramTokenFilter are maxGrams length, at most,
       *    by truncating.
       * 5. Truncate number of tokens produced to prevent OOM (HELP-82120)
       */
      builder
          .appendBiTriGramShingleFilter()
          .appendEdgeNgramTokenFilter(
              this.analyzerSpecification.getMinGrams(),
              this.analyzerSpecification.getMaxGrams(),
              true)
          .appendTruncateFilter(this.analyzerSpecification.getMaxGrams());

      if (this.enableAutocompleteTruncateTokens) {
        builder.appendLimitTokenCountFilter(this.maxTokens);
      }

      return builder.build();
    }

    private TokenStreamComponents wrapComponentsForNgram(TokenStreamComponents components) {
      CustomTokenStreamBuilder builder = CustomTokenStreamBuilder.builder(components);

      // 1. Strip diacritics, if configured to do so.
      if (this.analyzerSpecification.isFoldDiacritics()) {
        builder.appendDiacriticStripping();
      }

      /*
       * 2. Group words, as tokenized by the user-specified analyzer, into groups of two or three.
       *    We do this so that nGrams can span word boundaries.
       * 3. Apply the nGramTokenFilter, splitting sequences into many nGrams.
       * 4. Ensure nGrams passed through the nGramTokenFilter are maxGrams length, at most,
       *    by truncating.
       * 5. Truncate number of tokens produced to prevent OOM (HELP-82120)
       */
      builder
          .appendBiTriGramShingleFilter()
          .appendNgramFilter(
              this.analyzerSpecification.getMinGrams(),
              this.analyzerSpecification.getMaxGrams(),
              true)
          .appendTruncateFilter(this.analyzerSpecification.getMaxGrams());

      if (this.enableAutocompleteTruncateTokens) {
        builder.appendLimitTokenCountFilter(this.maxTokens);
      }

      return builder.build();
    }

    private TokenStreamComponents wrapComponentsForRightEdgeNgram(
        TokenStreamComponents components) {
      CustomTokenStreamBuilder builder = CustomTokenStreamBuilder.builder(components);

      // 1. Strip diacritics, if configured to do so.
      if (this.analyzerSpecification.isFoldDiacritics()) {
        builder.appendDiacriticStripping();
      }

      /*
       * 2. Group words, as tokenized by the user-specified analyzer, into groups of two or three.
       *    We do this so that edgeGrams can span word boundaries.
       * 3. Reverse characters in grouped tokens, so that characters that were formerly on the right
       *    edge of a token are now on the left.
       * 4. Apply the edgeGramTokenFilter, splitting sequences into many edgeGrams.
       * 5. Ensure edgeGrams passed through the edgeGramTokenFilter are maxGrams length, at most,
       *    by truncating.
       * 6. Truncate number of tokens produced to prevent OOM (HELP-82120)
       */
      builder
          .appendBiTriGramShingleFilter()
          .appendReverseFilter()
          .appendEdgeNgramTokenFilter(
              this.analyzerSpecification.getMinGrams(),
              this.analyzerSpecification.getMaxGrams(),
              true)
          .appendTruncateFilter(this.analyzerSpecification.getMaxGrams());

      if (this.enableAutocompleteTruncateTokens) {
        builder.appendLimitTokenCountFilter(this.maxTokens);
      }

      return builder.build();
    }
  }
}
