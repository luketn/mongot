package com.xgen.mongot.index.lucene.analyzer;

import com.google.errorprone.annotations.Var;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;

public class CustomAnalyzer extends Analyzer {
  private final List<AnalysisStep<Reader>> charFilterDefinitions;
  private final Supplier<Tokenizer> tokenizerDefinition;
  private final List<AnalysisStep<TokenStream>> tokenFilterDefinitions;

  /** For testing only. Use CustomAnalyzer.Builder instead. */
  CustomAnalyzer(
      List<AnalysisStep<Reader>> charFilterDefinitions,
      Supplier<Tokenizer> tokenizerDefinition,
      List<AnalysisStep<TokenStream>> tokenFilterDefinitions) {
    this.charFilterDefinitions = charFilterDefinitions;
    this.tokenizerDefinition = tokenizerDefinition;
    this.tokenFilterDefinitions = tokenFilterDefinitions;
  }

  @Override
  protected Reader initReader(String fieldName, Reader reader) {
    @Var Reader charFilter = reader;
    for (var charFilterDefinition : this.charFilterDefinitions) {
      charFilter = charFilterDefinition.create(charFilter);
    }
    return charFilter;
  }

  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    Tokenizer tokenizer = this.tokenizerDefinition.get();
    return new TokenStreamComponents(tokenizer, normalize(fieldName, tokenizer));
  }

  @Override
  protected TokenStream normalize(String fieldName, TokenStream in) {
    @Var TokenStream tokenStream = in;
    for (var filterDefinition : this.tokenFilterDefinitions) {
      tokenStream = filterDefinition.create(tokenStream);
    }
    return tokenStream;
  }

  @Override
  protected Reader initReaderForNormalization(String fieldName, Reader reader) {
    return initReader(fieldName, reader);
  }

  public static Builder builder(Supplier<Tokenizer> tokenizerSupplier) {
    return new Builder(tokenizerSupplier);
  }

  public static class Builder {
    private final List<AnalysisStep<Reader>> charFilters = new ArrayList<>();
    private final Supplier<Tokenizer> tokenizer;
    private final List<AnalysisStep<TokenStream>> tokenFilters = new ArrayList<>();

    public Builder(Supplier<Tokenizer> tokenizerSupplier) {
      this.tokenizer = tokenizerSupplier;
    }

    /** Add a charFilter to be used with this Analyzer. */
    public Builder charFilter(AnalysisStep<Reader> charFilter) {
      this.charFilters.add(charFilter);
      return this;
    }

    /** Adds charFilters to those to be used by this Analyzer. */
    public Builder charFilters(List<? extends AnalysisStep<Reader>> charFilters) {
      @Var Builder builder = this;
      for (var charFilter : charFilters) {
        builder = builder.charFilter(charFilter);
      }
      return builder;
    }

    /** Add a tokenFilter to be used by this Analyzer. */
    public Builder tokenFilter(AnalysisStep<TokenStream> tokenFilter) {
      this.tokenFilters.add(tokenFilter);
      return this;
    }

    /** Adds tokenFilters to those to be used by this Analyzer. */
    public Builder tokenFilters(List<? extends AnalysisStep<TokenStream>> tokenFilters) {
      @Var Builder builder = this;
      for (var tokenFilter : tokenFilters) {
        builder = builder.tokenFilter(tokenFilter);
      }
      return builder;
    }

    /** Build a CustomAnalyzer with this builder. */
    public CustomAnalyzer build() {
      return new CustomAnalyzer(this.charFilters, this.tokenizer, this.tokenFilters);
    }
  }
}
