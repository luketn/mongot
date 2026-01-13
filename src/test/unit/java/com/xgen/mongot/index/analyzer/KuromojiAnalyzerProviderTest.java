package com.xgen.mongot.index.analyzer;

import com.xgen.testing.mongot.index.analyzer.AnalyzerTestUtil;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.ja.JapaneseAnalyzer;
import org.junit.Assert;
import org.junit.Test;

public class KuromojiAnalyzerProviderTest {
  private static final String STOPWORDS_FILENAME = "japanese-analyzer-stopwords";
  private static final String COMMENT_IDENTIFIER = "#";

  @Test
  public void testStopwords() {
    var stopwords = AnalyzerTestUtil.readStopwordsFromFile(STOPWORDS_FILENAME, COMMENT_IDENTIFIER);
    Assert.assertEquals(new CharArraySet(stopwords, false), JapaneseAnalyzer.getDefaultStopSet());
  }
}
