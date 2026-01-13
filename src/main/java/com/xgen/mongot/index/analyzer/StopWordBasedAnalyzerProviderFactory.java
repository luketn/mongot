package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.analyzer.attributes.TokenStreamType;
import com.xgen.mongot.index.analyzer.definition.OverriddenBaseAnalyzerDefinition;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;

class StopWordBasedAnalyzerProviderFactory {

  static <T extends Analyzer> AnalyzerProvider.OverriddenBase create(
      String name,
      BiFunction<CharArraySet, CharArraySet, T> stopWordAndStemConstructor,
      Function<CharArraySet, T> stopWordConstructor,
      Supplier<T> defaultConstructor) {

    return new AnalyzerProvider.OverriddenBase() {
      @Override
      public T getAnalyzer(OverriddenBaseAnalyzerDefinition analyzerDefinition)
          throws InvalidAnalyzerDefinitionException {
        if (analyzerDefinition.getMaxTokenLength().isPresent()) {
          throw new InvalidAnalyzerDefinitionException(
              String.format("%s does not support maxTokenLength", name));
        }

        if (analyzerDefinition.getStemExclusionSet().isPresent()) {
          if (analyzerDefinition.getStopwords().isPresent()) {
            return stopWordAndStemConstructor.apply(
                analyzerDefinition.getStopwords().get(),
                analyzerDefinition.getStemExclusionSet().get());
          } else {
            throw new InvalidAnalyzerDefinitionException(
                String.format("%s must have Stopwords to support StemExclusionSet", name));
          }
        }

        if (analyzerDefinition.getStopwords().isPresent()) {
          return stopWordConstructor.apply(analyzerDefinition.getStopwords().get());
        }

        return defaultConstructor.get();
      }

      @Override
      public TokenStreamType getTokenStreamType() {
        return TokenStreamType.STREAM;
      }
    };
  }
}
