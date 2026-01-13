package com.xgen.testing.mongot.index.analyzer.custom;

import com.xgen.mongot.index.analyzer.custom.EdgeGramTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.KeywordTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.NGramTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.RegexCaptureGroupTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.RegexSplitTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.StandardTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.UaxUrlEmailTokenizerDefinition;
import com.xgen.mongot.index.analyzer.custom.WhitespaceTokenizerDefinition;
import com.xgen.mongot.util.Check;
import java.util.Optional;
import java.util.regex.Pattern;

public class TokenizerDefinitionBuilder {

  public static class EdgeGramTokenizer {
    private Optional<Integer> minGram = Optional.empty();
    private Optional<Integer> maxGram = Optional.empty();

    public static EdgeGramTokenizer builder() {
      return new EdgeGramTokenizer();
    }

    public EdgeGramTokenizer minGram(int minGram) {
      this.minGram = Optional.of(minGram);
      return this;
    }

    public EdgeGramTokenizer maxGram(int maxGram) {
      this.maxGram = Optional.of(maxGram);
      return this;
    }

    /** Builds an EdgeGramTokenizerDefinition. Throws an error if minGram or maxGram isn't set. */
    public EdgeGramTokenizerDefinition build() {
      return new EdgeGramTokenizerDefinition(
          Check.isPresent(this.minGram, "minGram"), Check.isPresent(this.maxGram, "maxGram"));
    }
  }

  public static class KeywordTokenizer {
    public static KeywordTokenizerDefinition build() {
      return new KeywordTokenizerDefinition();
    }
  }

  public static class NGramTokenizer {
    private Optional<Integer> minGram = Optional.empty();
    private Optional<Integer> maxGram = Optional.empty();

    public static NGramTokenizer builder() {
      return new NGramTokenizer();
    }

    public NGramTokenizer minGram(int minGram) {
      this.minGram = Optional.of(minGram);
      return this;
    }

    public NGramTokenizer maxGram(int maxGram) {
      this.maxGram = Optional.of(maxGram);
      return this;
    }

    /** Builds an EdgeGramTokenizerDefinition. Throws an error if minGram or maxGram isn't set. */
    public NGramTokenizerDefinition build() {
      return new NGramTokenizerDefinition(
          Check.isPresent(this.minGram, "minGram"), Check.isPresent(this.maxGram, "maxGram"));
    }
  }

  public static class StandardTokenizer {
    Optional<Integer> maxTokenLength = Optional.empty();

    public static StandardTokenizer builder() {
      return new StandardTokenizer();
    }

    public StandardTokenizer maxTokenLength(int maxTokenLength) {
      this.maxTokenLength = Optional.of(maxTokenLength);
      return this;
    }

    public StandardTokenizerDefinition build() {
      return new StandardTokenizerDefinition(this.maxTokenLength);
    }
  }

  public static class UaxUrlEmailTokenizer {
    Optional<Integer> maxTokenLength = Optional.empty();

    public static UaxUrlEmailTokenizer builder() {
      return new UaxUrlEmailTokenizer();
    }

    public UaxUrlEmailTokenizer maxTokenLength(int maxTokenLength) {
      this.maxTokenLength = Optional.of(maxTokenLength);
      return this;
    }

    public UaxUrlEmailTokenizerDefinition build() {
      return new UaxUrlEmailTokenizerDefinition(this.maxTokenLength);
    }
  }

  public static class WhitespaceTokenizer {
    Optional<Integer> maxTokenLength = Optional.empty();

    public static WhitespaceTokenizer builder() {
      return new WhitespaceTokenizer();
    }

    public WhitespaceTokenizer maxTokenLength(int maxTokenLength) {
      this.maxTokenLength = Optional.of(maxTokenLength);
      return this;
    }

    public WhitespaceTokenizerDefinition build() {
      return new WhitespaceTokenizerDefinition(this.maxTokenLength);
    }
  }

  public static class RegexCaptureGroupTokenizer {
    String pattern;
    int group;

    public static RegexCaptureGroupTokenizer builder() {
      return new RegexCaptureGroupTokenizer();
    }

    public RegexCaptureGroupTokenizer pattern(String pattern) {
      this.pattern = pattern;
      return this;
    }

    public RegexCaptureGroupTokenizer group(int group) {
      this.group = group;
      return this;
    }

    public RegexCaptureGroupTokenizerDefinition build() {
      return new RegexCaptureGroupTokenizerDefinition(Pattern.compile(this.pattern), this.group);
    }
  }

  public static class RegexSplitTokenizer {
    String pattern;

    public static RegexSplitTokenizer builder() {
      return new RegexSplitTokenizer();
    }

    public RegexSplitTokenizer pattern(String pattern) {
      this.pattern = pattern;
      return this;
    }

    public RegexSplitTokenizerDefinition build() {
      return new RegexSplitTokenizerDefinition(Pattern.compile(this.pattern));
    }
  }
}
