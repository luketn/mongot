package com.xgen.mongot.index.analyzer;

import java.io.Reader;
import java.util.function.Consumer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.icu.ICUFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.LimitTokenCountFilter;
import org.apache.lucene.analysis.miscellaneous.TruncateTokenFilter;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.ngram.NGramTokenFilter;
import org.apache.lucene.analysis.reverse.ReverseStringFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;

public class CustomTokenStreamBuilder {
  private Analyzer.TokenStreamComponents components;

  private CustomTokenStreamBuilder(Analyzer.TokenStreamComponents components) {
    this.components = components;
  }

  private TokenStream getTokenStream() {
    return this.components.getTokenStream();
  }

  private Consumer<Reader> getSource() {
    return this.components.getSource();
  }

  private CustomTokenStreamBuilder setComponents(Consumer<Reader> source, TokenStream tokenStream) {
    this.components = new Analyzer.TokenStreamComponents(source, tokenStream);
    return this;
  }

  static CustomTokenStreamBuilder builder(Analyzer.TokenStreamComponents components) {
    return new CustomTokenStreamBuilder(components);
  }

  CustomTokenStreamBuilder appendBiTriGramShingleFilter() {
    ShingleFilter shingleFilter = new ShingleFilter(getTokenStream(), 2, 3);
    shingleFilter.setOutputUnigramsIfNoShingles(true);
    return setComponents(getSource(), shingleFilter);
  }

  CustomTokenStreamBuilder appendDiacriticStripping() {
    return setComponents(getSource(), new ICUFoldingFilter(getTokenStream()));
  }

  CustomTokenStreamBuilder appendTruncateFilter(int max) {
    return setComponents(getSource(), new TruncateTokenFilter(getTokenStream(), max));
  }

  CustomTokenStreamBuilder appendLimitTokenCountFilter(int maxTokens) {
    return setComponents(getSource(), new LimitTokenCountFilter(getTokenStream(), maxTokens));
  }

  CustomTokenStreamBuilder appendEdgeNgramTokenFilter(
      int minNumGrams, int maxNumGrams, boolean preserveOriginal) {
    return setComponents(
        getSource(),
        new EdgeNGramTokenFilter(getTokenStream(), minNumGrams, maxNumGrams, preserveOriginal));
  }

  CustomTokenStreamBuilder appendNgramFilter(
      int minNumGrams, int maxNumGrams, boolean preserveOriginal) {
    return setComponents(
        getSource(),
        new NGramTokenFilter(getTokenStream(), minNumGrams, maxNumGrams, preserveOriginal));
  }

  CustomTokenStreamBuilder appendReverseFilter() {
    return setComponents(getSource(), new ReverseStringFilter(getTokenStream()));
  }

  public Analyzer.TokenStreamComponents build() {
    return this.components;
  }
}
