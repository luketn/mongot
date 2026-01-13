package com.xgen.mongot.index.analyzer;

import static com.xgen.testing.mongot.index.analyzer.AnalyzerTestUtil.testAnalyzerShouldProduceToken;
import static com.xgen.testing.mongot.index.analyzer.AnalyzerTestUtil.testShouldNotProduceToken;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.index.IndexWriter;
import org.junit.Test;

public class TokenByteSizeFilterTest {

  private static Analyzer getAnalyzer(int maxByteLength) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(
            tokenizer, new TokenByteSizeFilter(tokenizer, "foo", maxByteLength));
      }
    };
  }

  private static Analyzer getAnalyzer() {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new KeywordTokenizer();
        return new TokenStreamComponents(tokenizer, new TokenByteSizeFilter(tokenizer, "foo"));
      }
    };
  }

  @Test
  public void testEmptyTerm() throws Exception {
    try (Analyzer a = getAnalyzer(5)) {
      testAnalyzerShouldProduceToken(a, "");
    }
  }

  @Test
  public void testShorterTerm() throws Exception {
    try (Analyzer a = getAnalyzer(5)) {
      testAnalyzerShouldProduceToken(a, "aaaa");

      // ¢ is a 2 byte character
      testAnalyzerShouldProduceToken(a, "¢");
      testAnalyzerShouldProduceToken(a, "a¢");
      testAnalyzerShouldProduceToken(a, "aa¢");
      testAnalyzerShouldProduceToken(a, "¢¢");

      // € is a 3 byte character
      testAnalyzerShouldProduceToken(a, "€");
      testAnalyzerShouldProduceToken(a, "a€");

      // 𠜎 is a 4 byte character
      testAnalyzerShouldProduceToken(a, "𠜎");
    }
  }

  @Test
  public void testBoundaryTerm() throws Exception {
    try (Analyzer a = getAnalyzer(5)) {
      testAnalyzerShouldProduceToken(a, "aaaaa");

      // ¢ is a 2 byte character
      testAnalyzerShouldProduceToken(a, "aaa¢");
      testAnalyzerShouldProduceToken(a, "a¢¢");

      // € is a 3 byte character
      testAnalyzerShouldProduceToken(a, "aa€");

      // 𠜎 is a 4 byte character
      testAnalyzerShouldProduceToken(a, "a𠜎");
    }
  }

  @Test
  public void testLongTerm() throws Exception {
    try (Analyzer a = getAnalyzer(5)) {
      testShouldNotProduceToken(a, "aaaaaa");

      // ¢ is a 2 byte character
      testShouldNotProduceToken(a, "aaaa¢");
      testShouldNotProduceToken(a, "aa¢¢");
      testShouldNotProduceToken(a, "¢¢¢");

      // € is a 3 byte character
      testShouldNotProduceToken(a, "aaa€");
      testShouldNotProduceToken(a, "€€");

      // 𠜎 is a 4 byte character
      testShouldNotProduceToken(a, "aa𠜎");
      testShouldNotProduceToken(a, "𠜎𠜎");
    }
  }

  @Test
  public void testLuceneMaxTermSizeTerm() throws Exception {
    try (Analyzer a = getAnalyzer()) {
      // Term less than max.
      String oneLess = "a".repeat(IndexWriter.MAX_TERM_LENGTH - 1);
      testAnalyzerShouldProduceToken(a, oneLess);

      // Term same as max.
      String same = oneLess + "a";
      testAnalyzerShouldProduceToken(a, same);

      // Term larger than max.
      String larger = same + "a";
      testShouldNotProduceToken(a, larger);
    }
  }
}
