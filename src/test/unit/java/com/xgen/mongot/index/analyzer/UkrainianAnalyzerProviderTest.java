package com.xgen.mongot.index.analyzer;

import com.xgen.mongot.index.lucene.util.AnalyzedText;
import com.xgen.mongot.index.path.string.StringFieldPath;
import com.xgen.mongot.util.FieldPath;
import com.xgen.testing.mongot.index.analyzer.AnalyzerTestUtil;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.uk.UkrainianMorfologikAnalyzer;
import org.junit.Assert;
import org.junit.Test;

public class UkrainianAnalyzerProviderTest {
  private static final String STOPWORDS_FILENAME = "ukrainian-analyzer-stopwords";
  private static final String COMMENT_IDENTIFIER = "#";

  @Test
  public void testStopwords() {
    var stopwords = AnalyzerTestUtil.readStopwordsFromFile(STOPWORDS_FILENAME, COMMENT_IDENTIFIER);
    Assert.assertEquals(
        new CharArraySet(stopwords, false), UkrainianMorfologikAnalyzer.getDefaultStopwords());
  }

  @Test
  public void testStopwordsAreRemoved() throws IOException {
    List<String> stopwords =
        AnalyzerTestUtil.readStopwordsFromFile(STOPWORDS_FILENAME, COMMENT_IDENTIFIER);
    List<String> tokens =
        AnalyzedText.applyAnalyzer(
            new UkrainianMorfologikAnalyzer(),
            new StringFieldPath(FieldPath.parse("foo")),
            stopwords,
            Optional.empty());

    Assert.assertEquals(0, tokens.size());
  }
}
