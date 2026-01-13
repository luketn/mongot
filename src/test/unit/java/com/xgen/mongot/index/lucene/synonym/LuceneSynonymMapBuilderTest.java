package com.xgen.mongot.index.lucene.synonym;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.junit.Assert;
import org.junit.Test;

public class LuceneSynonymMapBuilderTest {

  @Test
  public void testEmptySynonymMapBuilder() throws Exception {
    SynonymAnalyzer synonymAnalyzer =
        (SynonymAnalyzer)
            LuceneSynonymMapBuilder.builder(new StandardAnalyzer(), "lucene.standard")
                .build()
                .analyzer;

    SynonymAnalyzer expected =
        SynonymAnalyzer.create(new StandardAnalyzer(), new SynonymMap.Builder().build());

    Assert.assertNotEquals(expected, synonymAnalyzer);
  }
}
