package com.xgen.mongot.index.analyzer.custom;

import com.xgen.testing.mongot.index.analyzer.custom.TokenizerDefinitionBuilder;
import com.xgen.testing.mongot.index.lucene.analyzer.TokenizerTestUtil;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class UaxUrlEmailTokenizerDefinitionTest {
  @Test
  public void testEmptyInput() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.UaxUrlEmailTokenizer.builder().build();

    TokenizerTestUtil.testTokenizerProducesTokens(tokenizer, "", Collections.emptyList());
  }

  @Test
  public void testSimpleEmailAddress() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.UaxUrlEmailTokenizer.builder().build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "mailto:drake@thesix.org", List.of("mailto", "drake@thesix.org"));
  }

  @Test
  public void testMaxTokenLength() throws Exception {
    var tokenizer =
        TokenizerDefinitionBuilder.UaxUrlEmailTokenizer.builder().maxTokenLength(5).build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "mailto:drake@thesix.org", List.of("mailt", "o:dra", "ke", "thesi", "x.org"));
  }

  @Test
  public void testUrlWithTokenLength() throws Exception {
    var tokenizer =
        TokenizerDefinitionBuilder.UaxUrlEmailTokenizer.builder().maxTokenLength(5).build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "mongodb.com", List.of("mongo", "db.co", "m"));
  }

  @Test
  public void testUrl() throws Exception {
    var tokenizer = TokenizerDefinitionBuilder.UaxUrlEmailTokenizer.builder().build();

    TokenizerTestUtil.testTokenizerProducesTokens(
        tokenizer, "http://mongodb.com", List.of("http://mongodb.com"));
  }
}
